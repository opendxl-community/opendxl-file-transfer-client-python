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


class FileSendResult(object):
    """
    Class which holds the result data from a file send attempt.
    """

    def __init__(self, file_id, size, hashes):
        self._file_id = file_id
        self._size = size
        self._hashes = hashes

    @property
    def file_id(self):
        """
        Id of the file

        :rtype: str
        """
        return self._file_id

    @property
    def size(self):
        """
        Size of the file

        :rtype: int
        """
        return self._size

    @property
    def hashes(self):
        """
        Hashes computed for the file. Each item in the return dictionary should
        have a key defined in the
        :class:`dxlfiletransferclient.constants.HashType`
        class and a corresponding value containing a hexstring computed for the
        hash type.

        :rtype: dict
        """
        return self._hashes

    def to_dict(self):
        """
        Return a dictionary representation of this result object.

        :return: The dictionary representation
        :rtype: dict
        """
        return {
            FileStoreProp.ID: self._file_id,
            FileStoreProp.SIZE: self._size,
            FileStoreProp.HASHES: self._hashes
        }


class FileSendSegmentResult(object):
    """
    Class which holds the result data from a file send segment request.
    """

    def __init__(self, file_id, segments_received,
                 file_result=FileStoreResultProp.NONE,
                 total_segments=None):
        self._file_id = file_id
        self._segments_received = segments_received
        self._file_result = file_result
        self._total_segments = total_segments

    @property
    def file_id(self):
        """
        Id of the file

        :rtype: str
        """
        return self._file_id

    @property
    def segments_received(self):
        """
        Number of segments received so far for the file

        :rtype: int
        """
        return self._segments_received

    @property
    def total_segments(self):
        """
        Total number of segments in the file. If the total number of segments is
        unknown, `None` is returned.

        :rtype: int
        """
        return self._total_segments

    @total_segments.setter
    def total_segments(self, total_segments):
        self._total_segments = total_segments

    @property
    def file_result(self):
        """
        Storage result for the entire file (not just the segment), a member of
        the :class:`dxlfiletransferclient.constants.FileStoreResultProp` class.
        If the stored segment was not the last one for the file, the return
        value would be
        :const:`dxlfiletransferclient.constants.FileStoreResultProp.NONE`.

        :rtype: str
        """
        return self._file_result

    @file_result.setter
    def file_result(self, file_result):
        self._file_result = file_result


class FileTransferClient(Client):
    """
    The "File Transfer DXL Python client" client wrapper class.
    """

    #: The DXL service type for the File Transfer API.
    _SERVICE_TYPE = "/opendxl-file-transfer/service/file-transfer"

    #: The default topic on the DXL fabric to send files to.
    _DEFAULT_FILE_SEND_TOPIC = "{}/file/store".format(_SERVICE_TYPE)

    #: The default segment size to use for file transfer operations
    _DEFAULT_MAX_SEGMENT_SIZE = 50 * (2 ** 10)  # 50 KB

    def __init__(self, dxl_client, send_file_topic=_DEFAULT_FILE_SEND_TOPIC):
        """
        Constructor parameters:

        :param dxlclient.client.DxlClient dxl_client: The DXL client to use for
            communication with the fabric.
        :param str send_file_topic: Topic name to use for file send operations.
        """
        super(FileTransferClient, self).__init__(dxl_client)
        self._dxl_client = dxl_client
        self._file_store_topic = send_file_topic

    def send_file_request(self, file_name_to_send, file_name_on_server=None,
                          max_segment_size=_DEFAULT_MAX_SEGMENT_SIZE,
                          callback=None):
        """
        Send the contents of a file as
        `request <https://opendxl.github.io/opendxl-client-python/pydoc/dxlclient.message.html#dxlclient.message.Request>`_
        messages to the DXL fabric. This method presumes that a service
        registered with the DXL fabric will provide response messages for the
        requests which are sent.

        Example:

        .. code-block:: python

            from dxlfiletransferclient import FileTransferClient
            from dxlclient.client import DxlClient

            # Create the client
            with DxlClient(config) as dxl_client:

                # Connect to the fabric
                dxl_client.connect()

                # Create client wrapper
                client = FileTransferClient(dxl_client)

                # Send the contents of a local file, "/root/localfile.txt", to
                # be stored remotely as a file named "stored.txt".
                resp = client.send_file_request("/root/localfile.txt",
                    "stored.txt")

        :param str file_name_to_send: Path to the locally accessible file
            which should be sent.
        :param str file_name_on_server: Name that the file should be stored as
            on the server. The name may contain subdirectories if it is desired
            to store the file in a subdirectory under the base storage
            directory on the server, for example, `localsubdir/stored.txt`.
            If no value is set for this parameter, the base name of the file
            specified in the `file_name_to_send` parameter is used. For
            example, if `file_name_on_server` were not specified but
            `file_name_to_send` were specified as `/root/localfile.txt`, the
            file stored at the server would be under the root storage
            directory, with a name of `localfile.txt`.
        :param int max_segment_size: Maximum size (in bytes) for each file
            segment transferred through the DXL fabric.
        :param function callback: Optional callable object called back upon with
            results for each transferred segment. The parameter passed into the
            callback should be a :class:`FileSendSegmentResult` instance.
        :return: The result of the send request.
        :rtype: FileSendResult
        """
        if not file_name_on_server:
            file_name_on_server = os.path.basename(file_name_to_send)

        file_size = os.path.getsize(file_name_to_send)
        total_segments = file_size // max_segment_size
        if file_size % max_segment_size:
            total_segments += 1

        with open(file_name_to_send, 'rb') as file_handle:
            return self.send_file_from_stream_request(
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
        """
        Populate the value used for the `other_fields` field in a file store
        request.

        :param str file_name_on_server: Name that the file should be stored as
            on the server. The name may contain subdirectories if it is desired
            to store the file in a subdirectory under the base storage
            directory on the server, for example, `localsubdir/stored.txt`.
        :param int segment_number: Number of the next file segment to send.
        :param str file_id: Id of the file.
        :param bytes segment: The contents of the next segment to be sent.
        :param int bytes_read: Number of bytes from the local stream to be
            forwarded which have been read so far.
        :param int stream_size: Total size of the local stream (`None` if not
            known).
        :param int total_segments: Total number of segments that the stream
            will be sent across in (`None` if not known).
        :param hashlib.HASH file_hash_sha256: A SHA-256 hash computed from the
            contents of the local stream which have been read so far.
        :return: The `other_fields` field content.
        :rtype: dict
        """
        other_fields = {
            FileStoreProp.SEGMENT_NUMBER: str(segment_number)
        }

        # The 'file_id' is sent back from the service in the response
        # for the first file segment. The 'file_id' must be included in
        # each subsequent file segment request.
        if file_id:
            other_fields[FileStoreProp.ID] = file_id

        # If all of the bytes in the local stream have been read, this must
        # be the last segment. Send a 'store' result and file 'size' and
        # sha256 'hash' values that the service can use to confirm that
        # the full contents of the file were transmitted properly.
        if (bytes_read == stream_size) or \
                (segment_number == total_segments) or \
                not segment:
            other_fields[FileStoreProp.RESULT] = \
                FileStoreResultProp.STORE
            other_fields[FileStoreProp.NAME] = file_name_on_server
            other_fields[FileStoreProp.SIZE] = str(bytes_read)
            other_fields[FileStoreProp.HASH_SHA256] = \
                file_hash_sha256.hexdigest()

        return other_fields

    def send_file_from_stream_request(  # pylint: disable=too-many-locals
            self, stream, file_name_on_server,
            stream_size=None,
            max_segment_size=_DEFAULT_MAX_SEGMENT_SIZE,
            total_segments=None,
            callback=None):
        """
        Send the contents of a stream as
        `request <https://opendxl.github.io/opendxl-client-python/pydoc/dxlclient.message.html#dxlclient.message.Request>`_
        messages to the DXL fabric. This method presumes that a service
        registered with the DXL fabric will provide response messages for the
        requests which are sent.

        Example:

        .. code-block:: python

            from io import BytesIO
            from dxlfiletransferclient import FileTransferClient
            from dxlclient.client import DxlClient

            # Create the client
            with DxlClient(config) as dxl_client:

                # Connect to the fabric
                dxl_client.connect()

                # Create client wrapper
                client = FileTransferClient(dxl_client)

                # Create a byte stream
                some_bytes = BytesIO(b'a long stream of bytes')

                # Send byte stream to the service, to be stored remotely as a
                # file named "stored.txt".
                resp = client.send_file_from_stream_request(
                    some_bytes, "stored.txt")

        :param stream: The IO stream from which to read bytes for the send
            request.
        :param str file_name_on_server: Name that the file should be stored as
            on the server. The name may contain subdirectories if it is desired
            to store the file in a subdirectory under the base storage
            directory on the server, for example, `localsubdir/stored.txt`.
        :param int stream_size: Total size of the local stream (`None` if not
            known).
        :param int max_segment_size: Maximum size (in bytes) for each file
            segment transferred through the DXL fabric.
        :param int total_segments: Total number of segments that the stream
            will be sent across in (`None` if not known).
        :param callback: Optional callable object called back upon with results
            for each transferred segment. The parameter passed into the callback
            should be an :class:`FileSendSegmentResult` instance.
        :return: The result of the send request.
        :rtype: FileSendResult
        """
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

                # If a result is included in the `other_fields` data, the
                # last file segment has been read and so this should be the
                # last request sent to the service.
                continue_reading = FileStoreProp.RESULT not in other_fields

                logger.debug(
                    "Sending segment '%d' %sfor file '%s', id '%s'",
                    segment_number,
                    "" if total_segments is None else "of '{}' ".format(
                        total_segments),
                    file_name_on_server,
                    file_id)

                # Send the next file segment to the service
                segment_response_dict = self._invoke_service(
                    self._file_store_topic,
                    segment,
                    other_fields
                )
                segment_response = FileSendSegmentResult(
                    segment_response_dict[FileStoreProp.ID],
                    segment_response_dict[FileStoreProp.SEGMENTS_RECEIVED],
                    file_result=segment_response_dict.get(FileStoreProp.RESULT)
                )

                # Retain the 'file_id' sent from the server so that it can be
                # included in subsequent segment requests sent to the server.
                if not file_id:
                    file_id = segment_response.file_id

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
                        segment_response.segments_received
                    )

                if callback:
                    segment_response.total_segments = total_segments
                    callback(segment_response)
        finally:
            # If an error occurred while sending file segments, attempt to send
            # one last 'cancellation' request to the service so that the service
            # can cleanup resources that it had created for the file - for
            # example, to delete any previously stored file segments.
            if file_id and not complete_sent:
                logger.info(
                    "Error occurred, canceling store for file '%s', id '%s'",
                    file_name_on_server, file_id)
                self._invoke_service(
                    self._file_store_topic,
                    "",
                    {
                        FileStoreProp.ID: file_id,
                        FileStoreProp.NAME: file_name_on_server,
                        FileStoreProp.RESULT: FileStoreResultProp.CANCEL
                    }
                )

        return FileSendResult(file_id, bytes_read,
                              {HashType.SHA256: file_hash_sha256.hexdigest()})

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

        # Set the full request parameters.
        request.payload = payload
        request.other_fields = other_fields

        # Perform a synchronous DXL request.
        response = self._dxl_sync_request(request)

        # Convert the JSON payload in the DXL response message to a Python
        # dictionary and return it.
        return MessageUtils.json_payload_to_dict(response)
