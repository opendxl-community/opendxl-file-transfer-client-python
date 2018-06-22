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

    #: The DXL topic fragment for the File Transfer "file upload create" method.
    _REQ_TOPIC_FILE_UPLOAD_CREATE = "file/upload/create"
    #: The DXL topic fragment for the File Transfer "file upload complete" method.
    _REQ_TOPIC_FILE_UPLOAD_COMPLETE = "file/upload/complete"
    #: The DXL topic fragment for the File Transfer "file upload segment" method.
    _REQ_TOPIC_FILE_UPLOAD_SEGMENT = "file/upload/segment"

    _PARAM_FILE_ID = "file_id"
    _PARAM_FILE_NAME = "name"
    _PARAM_FILE_SIZE = "size"
    _PARAM_FILE_HASH = "hash"
    _PARAM_FILE_SEGMENT_NUMBER = "segment_number"
    _PARAM_FILE_SEGMENT_RECEIVED = "segment_received"
    _PARAM_FILE_SEGMENTS_REMAINING = "segments_remaining"
    _PARAM_FILE_TOTAL_SEGMENTS = "total_segments"
    _PARAM_FILE_CANCEL = "cancel"

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

    def upload_file(self, file_name_to_send, file_name_on_server,
                    max_segment_size=_DEFAULT_MAX_SEGMENT_SIZE):
        with open(file_name_to_send, 'rb') as file_handle:
            return self.upload_file_from_stream(
                file_handle,
                file_name_on_server,
                os.path.getsize(file_name_to_send),
                max_segment_size)

    def upload_file_from_stream(self, stream, file_name_on_server, stream_size,
                                max_segment_size=_DEFAULT_MAX_SEGMENT_SIZE):
        segments = stream_size // max_segment_size
        if stream_size % max_segment_size:
            segments += 1

        create_response = self._invoke_service(
            self._REQ_TOPIC_FILE_UPLOAD_CREATE,
            MessageUtils.dict_to_json({
                self._PARAM_FILE_NAME: file_name_on_server,
                self._PARAM_FILE_SIZE: stream_size,
                self._PARAM_FILE_TOTAL_SEGMENTS: segments
            })
        )
        file_id = create_response[self._PARAM_FILE_ID]

        all_segments_uploaded = False
        file_hash = hashlib.md5()
        try:
            segment = stream.read(max_segment_size)
            segment_number = 0
            while segment:
                segment_number += 1
                segment_response = self._invoke_service(
                    self._REQ_TOPIC_FILE_UPLOAD_SEGMENT,
                    segment,
                    {
                        self._PARAM_FILE_ID: file_id,
                        self._PARAM_FILE_SEGMENT_NUMBER: str(segment_number)
                    })
                logger.debug(
                    "Upload for id %s, segment received: '%d', remaining: '%d'",
                    file_id,
                    segment_response[self._PARAM_FILE_SEGMENT_RECEIVED],
                    segment_response[self._PARAM_FILE_SEGMENTS_REMAINING]
                )
                try:
                    file_hash.update(segment)
                except TypeError:
                    file_hash.update(segment.encode())
                segment = stream.read(max_segment_size)
            all_segments_uploaded = True
        finally:
            payload = {self._PARAM_FILE_ID: file_id}
            if all_segments_uploaded:
                payload[self._PARAM_FILE_HASH] = file_hash.hexdigest()
            else:
                payload[self._PARAM_FILE_CANCEL] = True
            self._invoke_service(self._REQ_TOPIC_FILE_UPLOAD_COMPLETE,
                                 MessageUtils.dict_to_json(payload))
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
