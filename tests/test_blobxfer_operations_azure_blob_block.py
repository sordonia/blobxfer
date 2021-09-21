# coding=utf-8
"""Tests for operations: block blob"""

# stdlib imports
import unittest.mock as mock
# non-stdlib imports
import azure.storage.common
# local imports
import blobxfer.version
import blobxfer.models.azure
# module under test
import blobxfer.operations.azure as azops
import blobxfer.operations.azure.blob.block as ops


def test_create_client():
    to = mock.MagicMock()
    to.max_retries = None
    proxy = mock.MagicMock(name='proxy')

    sa = azops.StorageAccount(
        'name', 'AAAAAA==', 'core.windows.net', 10, to, proxy=proxy)
    client = ops.create_client(sa, to, proxy=proxy)
    assert client is not None
    assert isinstance(client, azure.storage.blob._blob_service_client.BlobServiceClient)
    assert isinstance(
        client.credential,
        azure.storage.blob._shared.authentication.SharedKeyCredentialPolicy)
    assert client._config.user_agent_policy._user_agent.startswith(
        'blobxfer/{}'.format(blobxfer.version.__version__))
    assert client._config.proxy_policy.proxies is not None

    sa = azops.StorageAccount(
        'name', '?key&sig=key', 'core.windows.net', 10, to, None)
    client = ops.create_client(sa, to, None)
    assert client is not None
    assert isinstance(client, azure.storage.blob._blob_service_client.BlobServiceClient)
    assert sa.is_sas
    assert client.url.endswith("?sig=key")
    assert client._config.user_agent_policy._user_agent.startswith(
        'blobxfer/{}'.format(blobxfer.version.__version__))
    assert client._config.proxy_policy.proxies is None


def test_format_block_id():
    assert '00000001' == ops._format_block_id(1)


def test_put_block_from_url():
    dst_ase = mock.MagicMock()
    dst_ase.client.put_block_from_url = mock.MagicMock()

    src_ase = mock.MagicMock()
    src_ase.name = 'src_ase_name'
    src_ase.client.account_name = 'name'
    src_ase.container = 'container'
    src_ase.path = 'https://host/remote/path'
    src_ase.is_arbitrary_url = True

    offsets = mock.MagicMock()
    offsets.chunk_num = 0

    ops.put_block_from_url(src_ase, dst_ase, offsets)
    assert dst_ase.client.get_container_client().get_blob_client().stage_block_from_url.call_count == 1

    src_ase.is_arbitrary_url = False

    # azurite well-known account key
    src_ase.client.account_key = 'Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=='
    src_ase.client.generate_blob_shared_access_signature.return_value = 'sas'

    ops.put_block_from_url(src_ase, dst_ase, offsets)
    assert dst_ase.client.get_container_client().get_blob_client().stage_block_from_url.call_count == 2

    src_ase.client.account_key = None
    src_ase.client.sas_token = 'sastoken'

    ops.put_block_from_url(src_ase, dst_ase, offsets)
    assert dst_ase.client.get_container_client().get_blob_client().stage_block_from_url.call_count == 3

    src_ase.client.account_key = 'key'
    src_ase.client.sas_token = None
    src_ase.mode = blobxfer.models.azure.StorageModes.File
    src_ase.client.generate_file_shared_access_signature.return_value = 'sas'

    ops.put_block_from_url(src_ase, dst_ase, offsets)
    assert dst_ase.client.get_container_client().get_blob_client().stage_block_from_url.call_count == 4


def test_put_block_list():
    ase = mock.MagicMock()
    ase.name = 'abc'
    ops.put_block_list(ase, 1, None, None)
    assert ase.client.put_block_list.call_count == 1


def test_get_committed_block_list():
    ase = mock.MagicMock()
    ase.name = 'abc'
    gbl = mock.MagicMock()
    gbl.committed_blocks = 1
    ase.client.get_block_list.return_value = gbl
    assert ops.get_committed_block_list(ase) == 1

    ase.name = 'abc?snapshot=123'
    gbl.committed_blocks = 2
    assert ops.get_committed_block_list(ase) == 2
