# sentry-tablestore

An extension for Sentry which support [Alicloud Tablestore (OTS)](https://www.alibabacloud.com/product/table-store)
as a [node storage](https://develop.sentry.dev/services/nodestore/) backend.

## Objectives

Sentry provides an abstraction called ‘nodestore’ which is used for storing key/value blobs.

It's implemented by [BigTable](https://cloud.google.com/bigtable) (a service of Google Cloud Platform)
in [sentry.io](https://sentry.io/).

But for self-hosted Sentry, it's implemented by Django default.
This means the large amounts of key-value data were stored in the SQL database.

It's horrible, it can lead to rapid growth in the size of SQL database, and make it difficult to clean up.

For more details from https://forum.sentry.io/t/postgres-nodestore-node-table-124gb/12753/3 .

Therefore, this project came into being.
It provides a new solution in addition to Django Database or BigTable,
that is [Alicloud Tablestore (OTS)](https://www.alibabacloud.com/product/table-store).

## Features

- [x] Implement the TablestoreNodeStorage backend.
- [ ] Support migrating data from current nodestore backend to the new one.

## Prerequisites
Sentry 21.9.0 or higher.

An Tablestore instance of [Alibaba Cloud (International)](https://www.alibabacloud.com/product/table-store)
or [Aliyun (China)](https://www.aliyun.com/product/ots?source=5176.11533457&userCode=wh20sycz).

## Installation
To install, simply add `sentry-tablestore` to your *requirements.txt* for your Sentry environment (or `pip install sentry-tablestore`).

## Configuration
To configure Sentry to use this module, set `sentry_tablestore.nodestore.backend.TablestoreNodeStorage` to your `SENTRY_NODESTORE` in your *sentry.conf.py*, like this:

```python
from datetime import timedelta

SENTRY_NODESTORE = 'sentry_tablestore.nodestore.backend.TablestoreNodeStorage'
SENTRY_NODESTORE_OPTIONS = {
    # Get help from https://www.alibabacloud.com/help/tablestore/latest/python-sdk-initialization
    'end_point': '',
    'access_key_id': '',
    'access_key_secret': '',
}
```

Then, add any applicable configuration options. Depending on your environment, and especially if you are running Sentry in containers, you might consider using [python-decouple](https://pypi.python.org/pypi/python-decouple) so you can set these options via environment variables.

### Example Configuration

```Python
from datetime import timedelta
SENTRY_NODESTORE = 'sentry_tablestore.nodestore.backend.TablestoreNodeStorage'
SENTRY_NODESTORE_OPTIONS = {
    # Auto clean data for 90 days from its creation
    'default_ttl': timedelta(days=90),
    'automatic_expiry': True,

    # Get help from https://www.alibabacloud.com/help/tablestore/latest/python-sdk-initialization
    'end_point': 'https://sentry-self-hosted.cn-shanghai.vpc.tablestore.aliyuncs.com',
    'access_key_id': 'LTAI****************LYhz',
    'access_key_secret': 'MKs5**********************6t0J',

    # A boolean whether to enable zlib-compression, or the string "zstd" to use zstd.
    # Use zstd compression to override the default zlib for more compact and more efficient
    'compression': 'zstd'
}

# Set log-level for debugging
import logging
logger = logging.getLogger('sentry_tablestore.kvstore.tablestore') # Logger of this project
logger.setLevel(logging.DEBUG)
logger = logging.getLogger('tablestore-client') # Logger of Alicloud SDK
logger.setLevel(logging.DEBUG)
```

## Bootstrap

Execute the command to bootstrap tablestore (create or update the table):

```Shell
sentry upgrade --with-nodestore
```
