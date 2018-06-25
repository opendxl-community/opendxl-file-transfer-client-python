from __future__ import absolute_import
import hashlib
import logging
import os
from dxlclient.message import Request
from dxlbootstrap.util import MessageUtils
from dxlbootstrap.client import Client

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

    _PARAM_FILE_ID = "file_id"
    _PARAM_FILE_NAME = "name"
    _PARAM_FILE_SIZE = "size"
    _PARAM_FILE_HASH = "hash"
    _PARAM_FILE_SEGMENT_NUMBER = "segment_number"
    _PARAM_FILE_SEGMENTS_RECEIVED = "segments_received"
    _PARAM_FILE_RESULT = "result"
    _PARAM_FILE_CANCEL = "cancel"
    _PARAM_FILE_STORE = "store"

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

    def store_file(self, file_name_to_send, file_name_on_server,
                   max_segment_size=_DEFAULT_MAX_SEGMENT_SIZE):
        with open(file_name_to_send, 'rb') as file_handle:
            return self.store_file_from_stream(
                file_handle,
                file_name_on_server,
                os.path.getsize(file_name_to_send),
                max_segment_size)

    def store_file_from_stream(self, stream, file_name_on_server,
                               stream_size=None,
                               max_segment_size=_DEFAULT_MAX_SEGMENT_SIZE):
        file_hash = hashlib.md5()

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
                    other_fields[self._PARAM_FILE_ID] = file_id

                if segment:
                    bytes_read += len(segment)
                    try:
                        file_hash.update(segment)
                    except TypeError:
                        file_hash.update(segment.encode())
                if (bytes_read == stream_size) or not segment:
                    continue_reading = False
                    other_fields[self._PARAM_FILE_RESULT] = \
                        self._PARAM_FILE_STORE
                    other_fields[self._PARAM_FILE_SIZE] = str(bytes_read)
                    other_fields[self._PARAM_FILE_HASH] = file_hash.hexdigest()

                logger.debug(
                    "Sending segment '%d' for file '%s', id '%s'",
                    segment_number,
                    file_name_on_server,
                    file_id)

                segment_response = self._invoke_service(
                    self._REQ_TOPIC_FILE_STORE,
                    segment,
                    other_fields)

                if not file_id:
                    file_id = segment_response[self._PARAM_FILE_ID]

                if continue_reading:
                    logger.debug(
                        "Store of segment '%d' for file '%s', id '%s' succeeded",
                        segment_number,
                        file_name_on_server,
                        file_id
                    )
                else:
                    complete_sent = True
                    logger.info(
                        "Store for file %s, id '%s' complete, segments: '%d'",
                        file_name_on_server,
                        file_id,
                        segment_response[self._PARAM_FILE_SEGMENTS_RECEIVED]
                    )
        finally:
            if file_id and not complete_sent:
                logger.info(
                    "Error occurred, canceling store for file '%s', id '%s'",
                    file_name_on_server, file_id)
                self._invoke_service(
                    self._REQ_TOPIC_FILE_STORE,
                    "",
                    {
                        self._PARAM_FILE_ID: file_id,
                        self._PARAM_FILE_NAME: file_name_on_server,
                        self._PARAM_FILE_RESULT: self._PARAM_FILE_CANCEL
                    }
                )

        return {self._PARAM_FILE_ID: file_id}

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
