from __future__ import absolute_import
import hashlib
import logging
import os
from dxlclient.message import Request
from dxlbootstrap.util import MessageUtils
from dxlbootstrap.client import Client
from .constants import FileStoreProp, FileStoreResultProp, HashType

# Configure local logger
logger = logging.getLogger(__name__)


class FileTransferClient(Client):
    """
    The "File Transfer DXL Python client" client wrapper class.
    """

    #: The DXL service type for the File Transfer API.
    _SERVICE_TYPE = "/opendxl-file-transfer/service/file-transfer"

    #: The default topic on the DXL fabric to use for the File Transfer
    #: file store operations.
    _DEFAULT_FILE_TRANSFER_STORE_TOPIC = "{}/file/store".format(_SERVICE_TYPE)

    _DEFAULT_MAX_SEGMENT_SIZE = 1 * (2 ** 10)

    def __init__(self, dxl_client,
                 file_transfer_store_topic=_DEFAULT_FILE_TRANSFER_STORE_TOPIC):
        """
        Constructor parameters:

        :param dxlclient.client.DxlClient dxl_client: The DXL client to use for
            communication with the fabric.
        :param str file_transfer_store_topic: Unique id to use as part
            of the request topic names for the File Transfer DXL service.
        """
        super(FileTransferClient, self).__init__(dxl_client)
        self._dxl_client = dxl_client
        self._file_transfer_store_topic = file_transfer_store_topic

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

    @staticmethod
    def _create_request_other_fields(
            file_name_on_server, segment_number, file_id, segment, bytes_read,
            stream_size, total_segments, file_hash_sha256):

        other_fields = {
            FileStoreProp.NAME: file_name_on_server,
            FileStoreProp.SEGMENT_NUMBER: str(segment_number)
        }

        if file_id:
            other_fields[FileStoreProp.ID] = file_id

        if (bytes_read == stream_size) or \
                (segment_number == total_segments) or \
                not segment:
            other_fields[FileStoreProp.RESULT] = \
                FileStoreResultProp.STORE
            other_fields[FileStoreProp.SIZE] = str(bytes_read)
            other_fields[FileStoreProp.HASH_SHA256] = \
                file_hash_sha256.hexdigest()

        return other_fields

    def store_file_from_stream( # pylint: disable=too-many-locals
            self, stream, file_name_on_server,
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

                if segment:
                    bytes_read += len(segment)
                    try:
                        file_hash_sha256.update(segment)
                    except TypeError:
                        file_hash_sha256.update(segment.encode())

                other_fields = self._create_request_other_fields(
                    file_name_on_server, segment_number, file_id,
                    segment, bytes_read, stream_size, total_segments,
                    file_hash_sha256
                )

                continue_reading = FileStoreProp.RESULT not in other_fields

                logger.debug(
                    "Sending segment '%d' %sfor file '%s', id '%s'",
                    segment_number,
                    "" if total_segments is None else "of '{}' ".format(
                        total_segments),
                    file_name_on_server,
                    file_id)

                segment_response = self._invoke_service(
                    self._file_transfer_store_topic,
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
                    self._file_transfer_store_topic,
                    "",
                    {
                        FileStoreProp.ID: file_id,
                        FileStoreProp.NAME: file_name_on_server,
                        FileStoreProp.RESULT: FileStoreResultProp.CANCEL
                    }
                )

        return {
            FileStoreProp.ID: file_id,
            FileStoreProp.SIZE: bytes_read,
            FileStoreProp.HASHES: {
                HashType.SHA256: file_hash_sha256.hexdigest()
            }
        }

    def _invoke_service(self, topic, payload, other_fields=None):
        """
        Invokes a request method on the File Transfer DXL service.

        :param str topic: The topic to send the request to.
        :param payload: The payload to include in the request
        :param dict other_fields: Other fields to include in the request
        :return: Results of the service invocation.
        :rtype: dict
        """
        # Create the DXL request message.
        request = Request(topic)

        # Set the payload on the request message.
        request.payload = payload

        request.other_fields = other_fields

        # Perform a synchronous DXL request.
        response = self._dxl_sync_request(request)

        # Convert the JSON payload in the DXL response message to a Python
        # dictionary and return it.
        return MessageUtils.json_payload_to_dict(response)
