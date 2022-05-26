import enum
import logging
import time
from datetime import timedelta
from threading import Lock
from typing import Any, Iterator, Mapping, Optional, Sequence, Tuple, List, TypeVar

from tablestore import *

from sentry.utils.codecs import Codec, ZlibCodec, ZstdCodec
from sentry.utils.kvstore.abstract import KVStorage

logger = logging.getLogger(__name__)

_T = TypeVar('_T')

class TablestoreError(Exception):
    pass

class TablestoreKVStorage(KVStorage[str, bytes]):
    # There is a general limit that allow max 2MB of the value
    # https://www.alibabacloud.com/help/tablestore/latest/general-limits
    max_size = 1024 * 1024 * 2

    class Flags(enum.IntFlag):
        # XXX: Compression flags are assumed to be mutually exclusive, the
        # behavior is explicitly undefined if both bits are set on a record.
        COMPRESSED_ZLIB = 1 << 0
        COMPRESSED_ZSTD = 1 << 1

    compression_strategies: Mapping[str, Tuple[Flags, Codec[bytes, bytes]]] = {
        "zlib": (Flags.COMPRESSED_ZLIB, ZlibCodec()),
        "zstd": (Flags.COMPRESSED_ZSTD, ZstdCodec()),
    }

    def __init__(
        self,
        instance: str,
        table_name: str,
        client_options: Optional[Mapping[Any, Any]] = None,
        default_ttl: Optional[timedelta] = None,
        compression: Optional[str] = None,
        reserved_throughput: Optional[ReservedThroughput] = None,
    ) -> None:
        client_options = client_options if client_options is not None else {}

        if compression is not None and compression not in self.compression_strategies:
            raise ValueError(f'"compression" must be one of {self.compression_strategies.keys()!r}')

        self.instance = instance
        self.table_name = table_name
        self.client_options = client_options
        self.default_ttl = default_ttl
        self.compression = compression
        self.reserved_throughput = reserved_throughput or ReservedThroughput(CapacityUnit(0, 0))

        self.__client: OTSClient
        self.__client_lock = Lock()

    def _get_client(self) -> OTSClient:
        try:
            # Fast check for an existing client
            return self.__client
        except AttributeError:
            # If missing, we acquire our lock to initialize a new one
            with self.__client_lock:
                # It's possible that the lock was blocked waiting on someone
                # else who already initialized, so we first check again to make
                # sure this isn't the case.
                try:
                    client = self.__client
                except AttributeError:
                    client = self.__client = OTSClient(instance_name=self.instance, **self.client_options)
            return client

    def _get_table_or_none(self) -> Optional[DescribeTableResponse]:
        try:
            table = self._get_client().describe_table(self.table_name)
            return table
        except OTSServiceError as e:
            logger.debug("Failed to describe table (%s) with (%s).", self.table_name, e)
            return None

    @staticmethod
    def __chunk(arr: Sequence[_T], size: int) -> Iterator[Sequence[_T]]:
        for i in range(0, len(arr), size):
            yield arr[i: i + size]

    def get(self, key: str) -> Optional[bytes]:
        try:
            _, row, _ = self._get_client().get_row(self.table_name, self.__tuple_key(key), )
        except OTSServiceError as e:
            logger.debug("Failed to get row (%s) with (%s).", key, e)
            return None

        return self.__decode_row(row)

    def get_many(self, keys: Sequence[str]) -> Iterator[Tuple[str, bytes]]:
        failed = finished = 0
        # There is a general limit that allow max 100 rows read by one BatchGetRow request
        # https://www.alibabacloud.com/help/tablestore/latest/general-limits
        for chunk in self.__chunk(keys, 100):
            request = BatchGetRowRequest()
            request.add(TableInBatchGetRowItem(self.table_name, [self.__tuple_key(key) for key in chunk]))
            response = self._get_client().batch_get_row(request)
            for key, item in zip(chunk, response.get_result_by_table(self.table_name)):
                if item.is_ok:
                    value = self.__decode_row(item.row)
                    if value is not None:
                        yield item.row, value
                else:
                    e = item.error
                    logger.debug("Failed to get row (%s) with error (ErrorCode: %s, ErrorMessage: %s).", key, e.code, e.message)
                    failed += 1
            finished += len(chunk)
            logger.debug("Batch getting %d rows, %d finished with %d failed.", len(keys), finished, failed)

    def __decode_row(self, row: Row) -> Optional[bytes]:
        columns = {cell[0]: cell[1:] for cell in row.attribute_columns}

        data: Tuple[bytes, int]
        try:
            data = columns["data"]
        except KeyError:
            logger.warning("Retrieved row (%s) which does not contain a data column!", self.__str_key(row))
            return None

        # Ensure the row is in the life if the automatic_expiry option is off.
        # Notice: This checking logic may behave inconsistently with other backends.
        # The row-level TTL feature has been requested to drop via https://github.com/getsentry/sentry/issues/34132
        if self.default_ttl and len(data) > 1:
            if data[1] + self.default_ttl.total_seconds() * 1000 < time.time() * 1000:
                return None

        value = data[0]

        flags = columns.get("flags")[0]
        if flags:
            flags = self.Flags(flags)

            # Check if there is a compression flag set, if so decompress the value.
            # XXX: If no compression flags are matched, we unfortunately can't
            # tell the difference between data written with a compression
            # strategy that we're not aware of and data that was not compressed
            # at all, so we just return the data and hope for the best. It is
            # also possible that multiple compression flags match. We just stop
            # after the first one matches. Good luck!
            for compression_flag, strategy in self.compression_strategies.values():
                if compression_flag in flags:
                    value = strategy.decode(value)
                    break

        return value

    @staticmethod
    def __tuple_key(key: str) -> List[Tuple[str, str]]:
        return [("id", key)]

    @staticmethod
    def __str_key(row: Row) -> str:
        assert len(row.primary_key) == 1
        assert row.primary_key[0][0] == "id"
        return row.primary_key[0][1]

    def __row(self, key: str, data: bytes = None, flags: int = None) -> Row:
        columns: Optional[List[Tuple[str, Any]]] = None
        if data:
            columns = [("data", data)]
            # Only need to write the column at all if any flags are enabled.
            # And if so, pack it into a single byte.
            if flags:
                columns.append(("flags", flags))

        return Row(self.__tuple_key(key), columns)

    def set(self, key: str, value: bytes, ttl: Optional[timedelta] = None) -> None:
        if ttl is not None and ttl != self.default_ttl:
            raise NotImplementedError("TTL is not supported, via https://github.com/getsentry/sentry/issues/34132")

        # Track flags for metadata about this row. This only flag we're
        # tracking now is whether compression is on or not for the data column.
        flags = self.Flags(0)

        if self.compression:
            compression_flag, strategy = self.compression_strategies[self.compression]
            flags |= compression_flag
            value = bytearray(strategy.encode(value))

        assert len(value) <= self.max_size, f"Value size ({len(value)}) is larger than the general limit 2MB."

        row = self.__row(key, value, flags)

        try:
            self._get_client().put_row(self.table_name, row)
        except OTSServiceError as e:
            logger.debug("Failed to set row (%s) with (%s).", key, e)

    def delete(self, key: str) -> None:
        condition = Condition(RowExistenceExpectation.IGNORE)
        self._get_client().delete_row(self.table_name, self.__row(key), condition)
        logger.debug("Row (%s) has been deleted.", key)

    def delete_many(self, keys: Sequence[str]) -> None:
        errors = []

        condition = Condition(RowExistenceExpectation.IGNORE)

        finished = 0
        # There is a general limit that allow max 200 rows written by one BatchWriteRow request
        # https://www.alibabacloud.com/help/tablestore/latest/general-limits
        for chunk in self.__chunk(keys, 200):
            request = BatchWriteRowRequest()
            items = [DeleteRowItem(self.__row(key), condition) for key in chunk]
            request.add(TableInBatchWriteRowItem(self.table_name, items))
            response = self._get_client().batch_write_row(request)
            for status in response.get_failed_of_delete():
                errors.append(TablestoreError(status.error_code, status.error_message))
            finished += len(chunk)
            logger.debug("Batch deleting %d rows, %d finished with %d failed.", len(keys), finished, len(errors))

        if errors:
            raise TablestoreError(errors)

    def bootstrap(self, automatic_expiry: bool = True) -> None:
        client = self._get_client()
        table = self._get_table_or_none()

        schema_of_primary_key = [("id", "STRING")]
        table_meta = TableMeta(self.table_name, schema_of_primary_key)
        time_to_live = int(self.default_ttl.total_seconds()) if automatic_expiry else -1
        table_options = TableOptions(time_to_live, 1)

        if table is None:
            client.create_table(table_meta, table_options, self.reserved_throughput)
            logger.info("Table has been created.")
        else:
            actual_table_meta: TableMeta = table.table_meta
            if dict(actual_table_meta.schema_of_primary_key) != dict(schema_of_primary_key):
                logger.warning("Table (%s) is already exists but the metadata mismatched.", self.table_name)
            if table.table_options != table_options:
                client.update_table(self.table_name, table_options)
                logger.info("Table options has been updated.")
            if table.reserved_throughput_details != self.reserved_throughput:
                client.update_table(self.table_name, reserved_throughput=self.reserved_throughput)
                logger.info("Table reserved throughput has been updated.")

    def destroy(self) -> None:
        table = self._get_table_or_none()
        if table is None:
            logger.warning("Table (%s) does not exist, skip deleting.")
            return

        logger.debug("Deleting table (%s) ...", self.table_name)
        self._get_client().delete_table(self.table_name)
        logger.info("Table (%s) has been deleted.", self.table_name)
