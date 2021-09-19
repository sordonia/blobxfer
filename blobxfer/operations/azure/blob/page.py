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
import azure.storage.blob
# local imports
import blobxfer.retry
import blobxfer.util

# create logger
logger = logging.getLogger(__name__)


def create_client(storage_account, timeout, proxy):
    # type: (blobxfer.operations.azure.StorageAccount,
    #        blobxfer.models.options.Timeout,
    #        blobxfer.models.options.HttpProxy) -> PageBlobService
    """Create block blob client
    :param blobxfer.operations.azure.StorageAccount storage_account:
        storage account
    :param blobxfer.models.options.Timeout timeout: timeout
    :param blobxfer.models.options.HttpProxy proxy: proxy
    :rtype: PageBlobService
    :return: block blob service client
    """
    from blobxfer.operations.azure.blob.block import create_client
    return create_client(storage_account, timeout, proxy)


def create_blob(ase, timeout=None):
    # type: (blobxfer.models.azure.StorageEntity, int) -> None
    """Create page blob
    :param blobxfer.models.azure.StorageEntity ase: Azure StorageEntity
    :param int timeout: timeout
    """
    container: azure.storage.blob.ContainerClient = ase.client.get_container_client(ase.container)
    blob: azure.storage.blob.BlobClient = container.get_blob_client(ase.name)
    blob.create_page_blob(
        size=blobxfer.util.page_align_content_length(ase.size),
        content_settings=azure.storage.blob.models.ContentSettings(
            content_type=ase.content_type,
        ),
        timeout=timeout)


def put_page(ase, page_start, page_end, data, timeout=None):
    # type: (blobxfer.models.azure.StorageEntity,
    #        int, int, bytes, int) -> None
    """Puts a page into remote blob
    :param blobxfer.models.azure.StorageEntity ase: Azure StorageEntity
    :param int page_start: page range start
    :param int page_end: page range end
    :param bytes data: data
    :param int timeout: timeout
    """
    container: azure.storage.blob.ContainerClient = ase.client.get_container_client(ase.container)
    blob: azure.storage.blob.BlobClient = container.get_blob_client(ase.name)
    if data is None:
        data = b''
    blob.upload_page(
        data,
        offset=page_start,
        length=page_end - page_start,
        validate_content=False,  # integrity is enforced with HTTPS
        timeout=timeout
    )


def resize_blob(ase, size, timeout=None):
    # type: (blobxfer.models.azure.StorageEntity, int, int) -> None
    """Resizes a page blob
    :param blobxfer.models.azure.StorageEntity ase: Azure StorageEntity
    :param int size: content length
    :param int timeout: timeout
    """
    container: azure.storage.blob.ContainerClient = ase.client.get_container_client(ase.container)
    blob: azure.storage.blob.BlobClient = container.get_blob_client(ase.name)

    blob.resize_blob(
        blobxfer.util.page_align_content_length(size),
        timeout=timeout)  # noqa
