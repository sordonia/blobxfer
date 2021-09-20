# coding=utf-8
"""Tests for general blob operations"""

# stdlib imports
import unittest.mock as mock
# non-stdlib imports
import azure.common
import azure.storage.blob
import azure.core.exceptions
import pytest
# local imports
import blobxfer.models.azure as azmodels
# module under test
import blobxfer.operations.azure.blob as ops


class MockResponse:
    def __init__(self, reason, status_code):
        self.reason = reason
        self.status_code = status_code


def test_check_if_single_blob(mocker):
    client = mock.MagicMock()
    client.get_blob_properties.return_value = True

    result = ops.check_if_single_blob(client, 'a', 'b/c')
    assert result

    result = ops.check_if_single_blob(
        client, 'a', 'a?snapshot=2017-02-23T22:21:14.8121864Z')
    assert result

    client = mock.MagicMock()
    client.get_container_client().get_blob_client.side_effect = \
        azure.core.exceptions.ResourceNotFoundError('msg', MockResponse("not found", 404))

    result = ops.check_if_single_blob(client, 'a', 'b/c')
    assert not result


def test_get_blob_properties():
    with pytest.raises(RuntimeError):
        ops.get_blob_properties(
            None, 'cont', None, azmodels.StorageModes.File)

    client = mock.MagicMock()
    blob = mock.MagicMock()
    client.get_container_client().get_blob_client.side_effect = \
        azure.core.exceptions.ResourceNotFoundError('msg', MockResponse('code', 404))

    ret = ops.get_blob_properties(
        client, 'cont', None, azmodels.StorageModes.Append)
    assert ret is None

    blob = mock.MagicMock()
    blob.blob_type = azure.storage.blob._models.BlobType.PageBlob
    client = mock.MagicMock()
    client.get_container_client().get_blob_client().get_blob_properties.return_value = blob

    with pytest.raises(RuntimeError, match=".*PageBlob mismatch.*Append"):
        ops.get_blob_properties(
            client, 'cont', None, azmodels.StorageModes.Append)

    with pytest.raises(RuntimeError, match=".*PageBlob mismatch.*Block"):
        ops.get_blob_properties(
            client, 'cont', None, azmodels.StorageModes.Block)

    blob.blob_type = azure.storage.blob._models.BlobType.BlockBlob
    with pytest.raises(RuntimeError, match=".*BlockBlob mismatch.*Page"):
        ops.get_blob_properties(
            client, 'cont', None, azmodels.StorageModes.Page)

    ret = ops.get_blob_properties(
        client, 'cont', None, azmodels.StorageModes.Block)
    assert ret == blob


def test_list_blobs():
    with pytest.raises(RuntimeError):
        for blob in ops.list_blobs(
                None, 'cont', 'prefix', azmodels.StorageModes.File, True):
            pass

    _blob = azure.storage.blob._models.BlobProperties(name='dir/name')
    client = mock.MagicMock()
    client.get_container_client().list_blobs.return_value = [_blob]
    client.get_container_client().get_blob_client().get_blob_properties.return_value = _blob

    i = 0
    for blob in ops.list_blobs(
            client, 'cont', 'prefix', azmodels.StorageModes.Auto, False):
        i += 1
        assert blob.name == _blob.name
    assert i == 0

    i = 0
    for blob in ops.list_blobs(
            client, 'cont', 'prefix', azmodels.StorageModes.Auto, True):
        i += 1
        assert blob.name == _blob.name
    assert i == 1

    _blob.blob_type = \
        azure.storage.blob._models.BlobType.AppendBlob
    i = 0
    for blob in ops.list_blobs(
            client, 'dir', 'prefix', azmodels.StorageModes.Block, True):
        i += 1
        assert blob.name == _blob.name
    assert i == 0

    i = 0
    for blob in ops.list_blobs(
            client, 'dir', 'prefix', azmodels.StorageModes.Page, True):
        i += 1
        assert blob.name == _blob.name
    assert i == 0

    _blob.blob_type = \
        azure.storage.blob._models.BlobType.BlockBlob
    i = 0
    for blob in ops.list_blobs(
            client, 'dir', 'prefix', azmodels.StorageModes.Append, True):
        i += 1
        assert blob.name == _blob.name
    assert i == 0

    _blob.snapshot = '2017-02-23T22:21:14.8121864Z'
    client.get_blob_properties.return_value = _blob
    i = 0
    for blob in ops.list_blobs(
            client, 'cont',
            'a?snapshot=2017-02-23T22:21:14.8121864Z',
            azmodels.StorageModes.Auto,
            True):
        i += 1
        assert blob.name == _blob.name
        assert blob.snapshot == _blob.snapshot
    assert i == 1


def test_list_all_blobs():
    client = mock.MagicMock()
    blob = mock.MagicMock()
    client.get_container_client().list_blobs.return_value = [blob, blob]

    assert len(list(ops.list_all_blobs(client, 'cont'))) == 2


def test_get_blob_range():
    ase = mock.MagicMock(name='ase')
    ret = mock.MagicMock(name='ret')
    BYTES = b'\0'
    ret.content_as_bytes.return_value = BYTES
    ase.client.get_container_client().download_blob.return_value = ret
    ase.container = 'cont'
    ase.name = 'name'
    ase.snapshot = None
    offsets = mock.MagicMock()
    offsets.start_range = 0
    offsets.end_range = 1

    assert ops.get_blob_range(ase, offsets) == BYTES


def test_create_container():
    ase = mock.MagicMock()
    ase.can_create_containers = False

    ops.create_container(ase, None)
    assert ase.client.create_container.call_count == 0

    ase.can_create_containers = True
    ase.client.account_name = 'sa'
    ase.container = 'cont'

    cc = set()
    ase.client.create_container.return_value = True
    ops.create_container(ase, cc)
    assert len(cc) == 1

    ase.client.create_container.return_value = False
    ops.create_container(ase, cc)
    assert len(cc) == 1

    ase.container = 'cont2'
    ops.create_container(ase, cc)
    assert len(cc) == 2

    ops.create_container(ase, cc)
    assert len(cc) == 2
