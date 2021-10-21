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
from blobxfer.operations.azure.blob import get_blob_client_from_ase

# create logger
logger = logging.getLogger(__name__)


def create_client(storage_account, timeout, proxy):
    # type: (blobxfer.operations.azure.StorageAccount,
    #        blobxfer.models.options.Timeout,
    #        blobxfer.models.options.HttpProxy) -> AppendBlobService
    """Create Append blob client
    :param blobxfer.operations.azure.StorageAccount storage_account:
        storage account
    :param blobxfer.models.options.Timeout timeout: timeout
    :param blobxfer.models.options.HttpProxy proxy: proxy
    :rtype: AppendBlobService
    :return: append blob service client
    """
    from blobxfer.operations.azure.blob.block import create_client
    return create_client(storage_account, timeout, proxy)


def create_blob(ase, timeout=None):
    # type: (blobxfer.models.azure.StorageEntity, int) -> None
    """Create append blob
    :param blobxfer.models.azure.StorageEntity ase: Azure StorageEntity
    :param int timeout: timeout
    """
    get_blob_client_from_ase(ase).create_append_blob(
        content_settings=azure.storage.blob._models.ContentSettings(
            content_type=ase.content_type,
        ),
        timeout=timeout
    )  # noqa


def append_block(ase, data, timeout=None):
    # type: (blobxfer.models.azure.StorageEntity, bytes, int) -> None
    """Appends a block into remote blob
    :param blobxfer.models.azure.StorageEntity ase: Azure StorageEntity
    :param bytes data: data
    :param int timeout: timeout
    """
    get_blob_client_from_ase(ase).append_block(
        data or b'',
        validate_content=False,  # integrity is enforced with HTTPS
        timeout=timeout
    )  # noqa
