from __future__ import absolute_import
import hashlib
import logging
import os
from dxlclient.message import Request
from dxlbootstrap.util import MessageUtils
from dxlbootstrap.client import Client
from .constants import FileStoreProp, HashType

# Configure local logger
logger = logging.getLogger(__name__)


class FileTransferClient(Client):
    """
    The "File Transfer DXL Python client" client wrapper class.
    """

    #: The DXL service type for the File Transfer API.
    _SERVICE_TYPE = "/opendxl-file-transfer/service/file-transfer"

    #: The DXL topic fragment for the File Transfer "file store" method.
    _REQ_TOPIC_FILE_STORE = "file/store"

    _PARAM_FILE_HASH_SHA256 = "hash_sha256"
    _PARAM_FILE_NAME = "name"
    _PARAM_FILE_SEGMENT_NUMBER = "segment_number"

    _PARAM_FILE_RESULT = "result"
    _PARAM_FILE_RESULT_CANCEL = "cancel"
    _PARAM_FILE_RESULT_STORE = "store"

    _DEFAULT_MAX_SEGMENT_SIZE = 1 * (2 ** 10)

    def __init__(self, dxl_client, file_transfer_service_unique_id=None):
        """
        Constructor parameters:

        :param dxlclient.client.DxlClient dxl_client: The DXL client to use for
            communication with the fabric.
        :param str file_transfer_service_unique_id: Unique id to use as part
            of the request topic names for the File Transfer DXL service.
        """
        super(FileTransferClient, self).__init__(dxl_client)
        self._dxl_client = dxl_client
        self._file_transfer_service_unique_id = file_transfer_service_unique_id

    def store_file(self, file_name_to_send, file_name_on_server=None,
                   max_segment_size=_DEFAULT_MAX_SEGMENT_SIZE,
                   callback=None):
        if not file_name_on_server:
            file_name_on_server = os.path.basename(file_name_to_send)

        file_size = os.path.getsize(file_name_to_send)
        total_segments = file_size // max_segment_size
        if file_size % max_segment_size:
            total_segments += 1

        with open(file_name_to_send, 'rb') as file_handle:
            return self.store_file_from_stream(
                file_handle,
                file_name_on_server,
                os.path.getsize(file_name_to_send),
                max_segment_size,
                total_segments,
                callback
            )

    def store_file_from_stream(self, stream, file_name_on_server,
                               stream_size=None,
                               max_segment_size=_DEFAULT_MAX_SEGMENT_SIZE,
                               total_segments=None,
                               callback=None):
        file_hash_sha256 = hashlib.sha256()

        segment_number = 0
        file_id = None
        bytes_read = 0
        continue_reading = True
        complete_sent = False

        try:
            while continue_reading:
                segment_number += 1
                segment = stream.read(max_segment_size)

                other_fields = {
                    self._PARAM_FILE_NAME: file_name_on_server,
                    self._PARAM_FILE_SEGMENT_NUMBER: str(segment_number)
                }

                if file_id:
                    other_fields[FileStoreProp.ID] = file_id

                if segment:
                    bytes_read += len(segment)
                    try:
                        file_hash_sha256.update(segment)
                    except TypeError:
                        file_hash_sha256.update(segment.encode())
                if (bytes_read == stream_size) or \
                        (segment_number == total_segments) or \
                        not segment:
                    continue_reading = False
                    other_fields[self._PARAM_FILE_RESULT] = \
                        self._PARAM_FILE_RESULT_STORE
                    other_fields[FileStoreProp.SIZE] = str(bytes_read)
                    other_fields[self._PARAM_FILE_HASH_SHA256] = \
                        file_hash_sha256.hexdigest()

                logger.debug(
                    "Sending segment '%d' %sfor file '%s', id '%s'",
                    segment_number,
                    "" if total_segments is None else "of '{}' ".format(
                        total_segments),
                    file_name_on_server,
                    file_id)

                segment_response = self._invoke_service(
                    self._REQ_TOPIC_FILE_STORE,
                    segment,
                    other_fields)

                if not file_id:
                    file_id = segment_response[FileStoreProp.ID]

                if continue_reading:
                    logger.debug(
                        "Store of segment '%d' %sfor file '%s', id '%s' succeeded",
                        segment_number,
                        "" if total_segments is None else "of '{}' ".format(
                            total_segments),
                        file_name_on_server,
                        file_id
                    )
                else:
                    complete_sent = True
                    logger.info(
                        "Store for file %s, id '%s' complete, segments: '%d'",
                        file_name_on_server,
                        file_id,
                        segment_response[FileStoreProp.SEGMENTS_RECEIVED]
                    )
                if callback:
                    if total_segments is not None:
                        segment_response[FileStoreProp.TOTAL_SEGMENTS] = \
                            total_segments
                        callback(segment_response)
        finally:
            if file_id and not complete_sent:
                logger.info(
                    "Error occurred, canceling store for file '%s', id '%s'",
                    file_name_on_server, file_id)
                self._invoke_service(
                    self._REQ_TOPIC_FILE_STORE,
                    "",
                    {
                        FileStoreProp.ID: file_id,
                        self._PARAM_FILE_NAME: file_name_on_server,
                        self._PARAM_FILE_RESULT: self._PARAM_FILE_RESULT_CANCEL
                    }
                )

        return {
            FileStoreProp.ID: file_id,
            FileStoreProp.SIZE: bytes_read,
            FileStoreProp.HASHES: {
                HashType.SHA256: file_hash_sha256.hexdigest()
            }
        }

    def _invoke_service(self, method, payload, other_fields=None):
        """
        Invokes a request method on the File Transfer DXL service.

        :param str method: The request method to append to the
            topic for the request.
        :param payload:
        :param other_fields:
        :return: Results of the service invocation.
        :rtype: dict
        """
        if self._file_transfer_service_unique_id:
            request_service_id = "/{}".format(
                self._file_transfer_service_unique_id)
        else:
            request_service_id = ""

        # Create the DXL request message.
        request = Request("{}{}/{}".format(
            self._SERVICE_TYPE,
            request_service_id,
            method))

        # Set the payload on the request message.
        request.payload = payload

        request.other_fields = other_fields

        # Perform a synchronous DXL request.
        response = self._dxl_sync_request(request)

        # Convert the JSON payload in the DXL response message to a Python
        # dictionary and return it.
        return MessageUtils.json_payload_to_dict(response)
