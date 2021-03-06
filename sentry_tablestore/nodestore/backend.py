import os

import sentry_sdk

from sentry.nodestore.base import NodeStorage
from ..kvstore.tablestore import TablestoreKVStorage

class TablestoreNodeStorage(NodeStorage):
    """
    A Tablestore-based backend for storing node data.

    :param project: Passed to tablestore client
    :param instance: Passed to tablestore client
    :param table: Passed to tablestore client
    :param automatic_expiry: Whether to set tablestore GC rule.
    :param default_ttl: How many days keys should be stored (and considered
        valid for reading + returning)
    :param compression: A boolean whether to enable zlib-compression, or the
        string "zstd" to use zstd.

    >>> TablestoreNodeStorage(
    ...     instance='sentry',
    ...     table='nodestore',
    ...     default_ttl=timedelta(days=30),
    ...     compression=True,
    ... )
    """

    store_class = TablestoreKVStorage

    def __init__(
        self,
        instance="sentry",
        table="nodestore",
        automatic_expiry=False,
        default_ttl=None,
        compression=False,
        reserved_throughput=None,
        **client_options,
    ):
        if compression is True:
            compression = "zlib"
        elif compression is False:
            compression = None

        self.store = self.store_class(
            instance=instance,
            table_name=table,
            default_ttl=default_ttl,
            compression=compression,
            client_options=client_options,
            reserved_throughput=reserved_throughput
        )
        self.automatic_expiry = automatic_expiry
        self.skip_deletes = automatic_expiry and "_SENTRY_CLEANUP" in os.environ

    def _get_bytes(self, id):
        return self.store.get(id)

    def _get_bytes_multi(self, id_list):
        rv = {id: None for id in id_list}
        rv.update(self.store.get_many(id_list))
        return rv

    def _set_bytes(self, id, data, ttl=None):
        self.store.set(id, data, ttl)

    def delete(self, id):
        if self.skip_deletes:
            return

        with sentry_sdk.start_span(op="nodestore.tablestore.delete"):
            try:
                self.store.delete(id)
            finally:
                self._delete_cache_item(id)

    def delete_multi(self, id_list):
        if self.skip_deletes:
            return

        with sentry_sdk.start_span(op="nodestore.tablestore.delete_multi") as span:
            span.set_tag("num_ids", len(id_list))

            if len(id_list) == 1:
                self.delete(id_list[0])
                return

            try:
                self.store.delete_many(id_list)
            finally:
                self._delete_cache_items(id_list)

    def bootstrap(self):
        self.store.bootstrap(automatic_expiry=self.automatic_expiry)
