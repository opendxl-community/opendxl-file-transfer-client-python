from __future__ import absolute_import
from ._version import __version__
from .client import FileSendResult, FileSendSegmentResult, FileTransferClient
from .constants import *
from .store import FileStoreManager, FileStoreSegmentResult


def get_version():
    """
    Returns the version of the package

    :return: The version of the package
    """
    return __version__
