# Copyright (c) Microsoft Corporation
#
# All rights reserved.
#
# MIT License
#
# Permission is hereby granted, free of charge, to any person obtaining a
# copy of this software and associated documentation files (the "Software"),
# to deal in the Software without restriction, including without limitation
# the rights to use, copy, modify, merge, publish, distribute, sublicense,
# and/or sell copies of the Software, and to permit persons to whom the
# Software is furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED *AS IS*, WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
# FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
# DEALINGS IN THE SOFTWARE.

# stdlib imports
import logging
# non-stdlib imports
import azure.common
import azure.core.exceptions

# local imports
import blobxfer.models.azure
import blobxfer.util

# create logger
logger = logging.getLogger(__name__)


def check_if_single_blob(client, container, prefix, timeout=None):
    # type: (azure.storage.blob.BaseBlobService, str, str, int) -> bool
    """Check if prefix is a single blob or multiple blobs
    :param azure.storage.blob.BaseBlobService client: blob client
    :param str container: container
    :param str prefix: path prefix
    :param int timeout: timeout
    :rtype: bool
    :return: if prefix in container is a single blob
    """
    if blobxfer.util.blob_is_snapshot(prefix):
        return True
    try:
        container: azure.storage.blob.ContainerClient = client.get_container_client(container)
        blob: azure.storage.blob.BlobClient = container.get_blob_client(prefix)
        blob.get_blob_properties(
            timeout=timeout
        )
    except azure.core.exceptions.ResourceNotFoundError:
        return False
    return True


def get_blob_properties(client, container, prefix, mode, timeout=None):
    # type: (azure.storage.blob.BaseBlobService, str, str,
    #        blobxfer.models.azure.StorageModes, int) ->
    #        azure.storage.blob.models.Blob
    """Get blob properties
    :param azure.storage.blob.BaseBlobService client: blob client
    :param str container: container
    :param str prefix: path prefix
    :param blobxfer.models.azure.StorageModes mode: storage mode
    :param int timeout: timeout
    :rtype: azure.storage.blob.models.Blob
    :return: blob
    """
    if mode == blobxfer.models.azure.StorageModes.File:
        raise RuntimeError(
            'cannot list Azure Blobs with incompatible mode: {}'.format(
                mode))
    try:
        container: azure.storage.blob.ContainerClient = client.get_container_client(container)
        blob: azure.storage.blob.BlobClient = container.get_blob_client(prefix)
        blob_properties = blob.get_blob_properties(
            timeout=timeout
        )
    except azure.core.exceptions.ResourceNotFoundError:
        return None
    if ((mode == blobxfer.models.azure.StorageModes.Append and
         blob_properties.blob_type !=
         azure.storage.blob.models._BlobTypes.AppendBlob) or
            (mode == blobxfer.models.azure.StorageModes.Block and
             blob_properties.blob_type !=
             azure.storage.blob.models._BlobTypes.BlockBlob) or
            (mode == blobxfer.models.azure.StorageModes.Page and
             blob_properties.blob_type !=
             azure.storage.blob.models._BlobTypes.PageBlob)):
        raise RuntimeError(
            'existing blob type {} mismatch with mode {}'.format(
                blob_properties.blob_type, mode))
    return blob


def list_blobs(client, container, prefix, mode, recursive, timeout=None):
    # type: (azure.storage.blob.BaseBlobService, str, str,
    #        blobxfer.models.azure.StorageModes, bool, int) ->
    #        azure.storage.blob.models.Blob
    """List blobs in path conforming to mode
    :param azure.storage.blob.BaseBlobService client: blob client
    :param str container: container
    :param str prefix: path prefix
    :param blobxfer.models.azure.StorageModes mode: storage mode
    :param bool recursive: recursive
    :param int timeout: timeout
    :rtype: BlobProperties
    :return: generator of blobs properties
    """
    if mode == blobxfer.models.azure.StorageModes.File:
        raise RuntimeError('cannot list Azure Files from blob client')
    
    container: azure.storage.blob.ContainerClient = client.get_container_client(container)
    if blobxfer.util.blob_is_snapshot(prefix):
        base_blob, snapshot = blobxfer.util.parse_blob_snapshot_parameter(prefix)
        blob: azure.storage.blob.BlobClient = container.get_blob_client(prefix)
        blob_properties = blob.get_blob_properties(timeout=timeout)
        blob.properties = blob_properties
        yield blob
        return
    
    blob_properties = container.list_blobs(
        name_starts_with=prefix if blobxfer.util.is_not_empty(prefix) else None,
        include=['metadata'],
        timeout=timeout,
    )
    for blob in blob_properties:
        if (mode == blobxfer.models.azure.StorageModes.Append and
                blob.blob_type !=
                azure.storage.blob._models.BlobType.AppendBlob):
            continue
        elif (mode == blobxfer.models.azure.StorageModes.Block and
                blob.blob_type !=
                azure.storage.blob._models.BlobType.BlockBlob):
            continue
        elif (mode == blobxfer.models.azure.StorageModes.Page and
                blob.blob_type !=
                azure.storage.blob._models.BlobType.PageBlob):
            continue
        if not recursive and '/' in blob.name:
            continue
        # auto or match, yield the blob
        # return properties here!
        yield blob


def list_all_blobs(client, container, timeout=None):
    # type: (azure.storage.blob.BaseBlobService, str, int) ->
    #        azure.storage.blob.models.Blob
    """List all blobs in a container
    :param azure.storage.blob.BaseBlobService client: blob client
    :param str container: container
    :param int timeout: timeout
    :rtype: azure.storage.blob.models.Blob
    :return: generator of blobs
    """
       
    container: azure.storage.blob.ContainerClient = client.get_container_client(container)
    blob_properties = container.list_blobs(
        timeout=timeout,
    )
    for blob in blob_properties:
        yield blob_properties


def delete_blob(client, container, name, timeout=None):
    # type: (azure.storage.blob.BaseBlobService, str, str, int) -> None
    """Delete blob, including all associated snapshots
    :param azure.storage.blob.BaseBlobService client: blob client
    :param str container: container
    :param str name: blob name
    :param int timeout: timeout
    """
    container: azure.storage.blob.ContainerClient = client.get_container_client(container)
    container.delete_blob(
        name,
        delete_snapshots=azure.storage.blob.models.DeleteSnapshot.Include,
        timeout=timeout,
    )  # noqa


def get_blob_range(ase, offsets, timeout=None):
    # type: (blobxfer.models.azure.StorageEntity,
    #        blobxfer.models.download.Offsets, int) -> bytes
    """Retrieve blob range
    :param blobxfer.models.azure.StorageEntity ase: Azure StorageEntity
    :param blobxfer.models.download.Offsets offsets: download offsets
    :param int timeout: timeout
    :rtype: bytes
    :return: content for blob range
    """
    container: azure.storage.blob.ContainerClient = ase.client.get_container_client(ase.container)
    return container.download_blob(
        ase.name,
        offset=offsets.range_start,
        length=offsets.range_end - offsets.range_start + 1,
        validate_content=False,  # HTTPS takes care of integrity during xfer
        timeout=timeout,
    ).content_as_bytes()


def create_container(ase, containers_created, timeout=None):
    # type: (blobxfer.models.azure.StorageEntity, set, int) -> None
    """Create blob container
    :param blobxfer.models.azure.StorageEntity ase: Azure StorageEntity
    :param set containers_created: containers already created map
    :param int timeout: timeout
    """
    # check if auth allows create container
    if not ase.can_create_containers:
        return
    key = ase.client.account_name + ':blob=' + ase.container
    if key in containers_created:
        return
    try:
        ase.client.create_container(
            ase.container,
            timeout=timeout
        )
        logger.info(
            'created blob container {} on storage account {}'.format(
                ase.container, ase.client.account_name))
    except azure.core.exceptions.ResourceExistsError:
        pass
    finally:
        # always add to set (as it could be pre-existing)
        containers_created.add(key)


def set_blob_properties(ase, md5, timeout=None):
    # type: (blobxfer.models.azure.StorageEntity, str, int) -> None
    """Set blob properties
    :param blobxfer.models.azure.StorageEntity ase: Azure StorageEntity
    :param str md5: md5 as base64
    :param int timeout: timeout
    """

    container: azure.storage.blob.ContainerClient = ase.client.get_container_client(ase.container)
    blob: azure.storage.blob.BlobClient = container.get_blob_client(ase.name)
    blob.set_http_headers(
        content_settings=azure.storage.blob.models.ContentSettings(
            content_type=ase.content_type,
            content_md5=md5,
            cache_control=ase.cache_control,
        ),
        timeout=timeout
    )  # noqa


def set_blob_metadata(ase, metadata, timeout=None):
    # type: (blobxfer.models.azure.StorageEntity, dict, int) -> None
    """Set blob metadata
    :param blobxfer.models.azure.StorageEntity ase: Azure StorageEntity
    :param dict metadata: metadata kv pairs
    :param int timeout: timeout
    """
    container: azure.storage.blob.ContainerClient = ase.client.get_container_client(ase.container)
    blob: azure.storage.blob.BlobClient = container.get_blob_client(ase.name)
    blob.set_blob_metadata(
        metadata=metadata,
        timeout=timeout
    )  # noqa
