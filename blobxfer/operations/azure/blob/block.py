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
import datetime
import logging
# non-stdlib imports
import azure.storage.blob
# local imports
import blobxfer.models.azure
import blobxfer.retry

# create logger
logger = logging.getLogger(__name__)


def create_client(storage_account, timeout, proxy):
    # type: (blobxfer.operations.azure.StorageAccount,
    #        blobxfer.models.options.Timeout,
    #        blobxfer.models.options.HttpProxy) -> BlockBlobService
    """Create block blob client
    :param blobxfer.operations.azure.StorageAccount storage_account:
        storage account
    :param blobxfer.models.options.Timeout timeout: timeout
    :param blobxfer.models.options.HttpProxy proxy: proxy
    :rtype: azure.storage.blob.BlockBlobService
    :return: block blob service client
    """
    
    client = azure.storage.blob.BlobServiceClient(
        f"https://{storage_account.name}.blob.core.windows.net",
        credential=storage_account.key,
        session=storage_account.session,
        connection_timeout=timeout.timeout)

    # set proxy
    # if proxy is not None:
    #    client.set_proxy(
    #        proxy.host, proxy.port, proxy.username, proxy.password)

    # set retry policy
    client._config.retry_policy = blobxfer.retry.ExponentialRetryWithMaxWait(
        max_retries=timeout.max_retries).retry
    return client


def create_blob(ase, data, md5, metadata, timeout=None):
    # type: (blobxfer.models.azure.StorageEntity, bytes, str, dict,
    #        int) -> None
    """Create one shot block blob
    :param blobxfer.models.azure.StorageEntity ase: Azure StorageEntity
    :param bytes data: blob data
    :param str md5: md5 as base64
    :param dict metadata: metadata kv pairs
    :param int timeout: timeout
    """
    container: azure.storage.blob.ContainerClient = ase.client.get_container_client(ase.container)
    # need to decode for v12
    md5 = blobxfer.util.base64_decode_string(md5) if md5 else None
    # the previous sdk assumed b'' if data was None, replicate behavior here
    if data is None:
        return b''
    container.upload_blob(
        ase.name,
        data=data,
        content_settings=azure.storage.blob._models.ContentSettings(
            content_type=ase.content_type,
            content_md5=md5,
            cache_control=ase.cache_control,
        ),
        metadata=metadata,
        validate_content=False,  # integrity is enforced with HTTPS
        timeout=timeout
    )  # noqa


def _format_block_id(chunk_num):
    # type: (int) -> str
    """Create a block id given a block (chunk) number
    :param int chunk_num: chunk number
    :rtype: str
    :return: block id
    """
    return '{0:08d}'.format(chunk_num)


def put_block(ase, offsets, data, timeout=None):
    # type: (blobxfer.models.azure.StorageEntity,
    #        blobxfer.models.upload.Offsets, bytes, int) -> None
    """Puts a block into remote blob
    :param blobxfer.models.azure.StorageEntity ase: Azure StorageEntity
    :param blobxfer.models.upload.Offsets offsets: upload offsets
    :param bytes data: data
    :param int timeout: timeout
    """
    container: azure.storage.blob.ContainerClient = ase.client.get_container_client(ase.container)
    blob: azure.storage.blob.BlobClient = container.get_blob_client(ase.name)
    # the previous sdk assumed b'' if data was None, replicate behavior here
    if data is None:
        return b''
    blob.stage_block(
        data,
        block_id=_format_block_id(offsets.chunk_num),
        validate_content=False,  # integrity is enforced with HTTPS
        timeout=timeout)  # noqa


def put_block_from_url(src_ase, dst_ase, offsets, timeout=None):
    # type: (blobxfer.models.azure.StorageEntity,
    #        blobxfer.models.azure.StorageEntity,
    #        blobxfer.models.upload.Offsets, int) -> None
    """Puts a block into remote blob
    :param blobxfer.models.azure.StorageEntity src_ase:
        Source Azure StorageEntity
    :param blobxfer.models.azure.StorageEntity dst_ase:
        Destination Azure StorageEntity
    :param blobxfer.models.upload.Offsets offsets: upload offsets
    :param int timeout: timeout
    """
    if src_ase.is_arbitrary_url:
        src_url = src_ase.path
    else:
        if blobxfer.util.is_not_empty(src_ase.client.account_key):
            # TODO: handle generation of the keys here
            raise NotImplementedError

            if src_ase.mode == blobxfer.models.azure.StorageModes.File:
                sas = src_ase.client.generate_file_shared_access_signature(
                    share_name=src_ase.container,
                    file_name=src_ase.name,
                    permission=azure.storage.file.FilePermissions(read=True),
                    expiry=datetime.datetime.utcnow() + datetime.timedelta(
                        days=7),
                )
            else:
                sas = src_ase.client.generate_blob_shared_access_signature(
                    container_name=src_ase.container,
                    blob_name=src_ase.name,
                    permission=azure.storage.blob.BlobPermissions(read=True),
                    expiry=datetime.datetime.utcnow() + datetime.timedelta(
                        days=7),
                )
        else:
            sas = src_ase.client.sas_token
        src_url = 'https://{}/{}?{}'.format(
            src_ase.client.primary_endpoint, src_ase.path, sas)

    container: azure.storage.blob.ContainerClient = dst_ase.client.get_container_client(dst_ase.container)
    blob: azure.storage.blob.BlobClient = container.get_blob_client(dst_ase.name)
    
    blob.stage_block_from_url(
        block_id=_format_block_id(offsets.chunk_num),
        source_url=src_url,
        source_offset=offsets.range_start,
        source_length=offsets.range_end - offsets.range_start,
        source_content_md5=None,
        timeout=timeout
    )  # noqa


def put_block_list(
        ase, last_block_num, md5, metadata, timeout=None):
    # type: (blobxfer.models.azure.StorageEntity, bytes, str, dict,
    #        int) -> None
    """Create block blob from blocks
    :param blobxfer.models.azure.StorageEntity ase: Azure StorageEntity
    :param int last_block_num: last block number (chunk_num)
    :param str md5: md5 as base64
    :param dict metadata: metadata kv pairs
    :param int timeout: timeout
    """
    # construct block list
    block_list = [
        azure.storage.blob.BlobBlock(id=_format_block_id(x))
        for x in range(0, last_block_num + 1)
    ]
    container: azure.storage.blob.ContainerClient = ase.client.get_container_client(ase.container)
    blob: azure.storage.blob.BlobClient = container.get_blob_client(ase.name)
    md5 = blobxfer.util.base64_decode_string(md5) if md5 else None

    blob.commit_block_list(
        block_list=block_list,
        content_settings=azure.storage.blob.models.ContentSettings(
            content_type=ase.content_type,
            content_md5=md5,
            cache_control=ase.cache_control,
        ),
        metadata=metadata,
        validate_content=False,  # integrity is enforced with HTTPS
        timeout=timeout
    )


def get_committed_block_list(ase, timeout=None):
    # type: (blobxfer.models.azure.StorageEntity, int) -> list
    """Get committed block list
    :param blobxfer.models.azure.StorageEntity ase: Azure StorageEntity
    :param int timeout: timeout
    :rtype: list
    :return: list of committed blocks
    """
    if blobxfer.util.blob_is_snapshot(ase.name):
        blob_name, snapshot = blobxfer.util.parse_blob_snapshot_parameter(
            ase.name)
    else:
        blob_name = ase.name
        snapshot = None

    container: azure.storage.blob.ContainerClient = ase.client.get_container_client(ase.container)
    blob: azure.storage.blob.BlobClient = container.get_blob_client(blob_name)

    # committed type is already the default, [0] is committed blocks
    return blob.get_block_list(snapshot=snapshot, timeout=timeout)[0]


def set_blob_access_tier(ase, timeout=None):
    # type: (blobxfer.models.azure.StorageEntity, int) -> None
    """Set blob access tier
    :param blobxfer.models.azure.StorageEntity ase: Azure StorageEntity
    :param int timeout: timeout
    """
    container: azure.storage.blob.ContainerClient = ase.client.get_container_client(ase.container)
    blob: azure.storage.blob.BlobClient = container.get_blob_client(ase.name)

    blob.set_standard_blob_tier(
        standard_blob_tier=ase.access_tier,
        timeout=timeout
    )  # noqa
