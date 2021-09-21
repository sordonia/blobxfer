# coding=utf-8
"""Tests for models"""

# stdlib imports
import unittest.mock as mock
# non-stdlib imports
import azure.storage.common
# local imports
import blobxfer.version
# module under test
import blobxfer.operations.azure as azops
import blobxfer.operations.azure.blob.page as ops


def test_create_client():
    to = mock.MagicMock()
    to.max_retries = None

    sa = azops.StorageAccount(
        'name', 'AAAAAA==', 'core.windows.net', 10, to, mock.MagicMock())
    client = ops.create_client(sa, to, mock.MagicMock())
    assert client is not None
    assert isinstance(client, azure.storage.blob.BlobServiceClient)
    assert isinstance(
        client.credential,
        azure.storage.blob._shared.authentication.SharedKeyCredentialPolicy)
    assert client._config.user_agent_policy.user_agent.startswith(
        'blobxfer/{}'.format(blobxfer.version.__version__))
    assert client._config.proxy_policy.proxies is not None

    sa = azops.StorageAccount(
        'name', '?key&sig=key', 'core.windows.net', 10, to, None)
    client = ops.create_client(sa, to, None)
    assert client is not None
    assert isinstance(client, azure.storage.blob.BlobServiceClient)
    assert client.credential is None
    assert sa.is_sas
    assert client.url.endswith('?sig=key')
    assert client._config.user_agent_policy.user_agent.startswith(
        'blobxfer/{}'.format(blobxfer.version.__version__))
    assert client._config.proxy_policy.proxies is None
