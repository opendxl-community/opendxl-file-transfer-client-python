from __future__ import absolute_import
import hashlib
import logging
import os
import shutil
import threading
import uuid
from .constants import FileStoreProp, FileStoreResultProp

# Configure local logger
logger = logging.getLogger(__name__)


def _get_value_as_int(dict_obj, key):
    """
    Return the value associated with a key in a dictionary, converted to an int.

    :param dict dict_obj: Dictionary to retrieve the value from
    :param str key: Key associated with the value to return
    :return The value, as an integer. Returns 'None' if the key cannot be found
        in the dictionary.
    :rtype: int
    :raises ValueError: If the key is present in the dictionary but the value
        cannot be converted to an int.
    """
    return_value = None
    if key in dict_obj:
        try:
            return_value = int(dict_obj.get(key))
        except ValueError:
            raise ValueError(
                "'{}' of '{}' could not be converted to an int".format(
                    key, return_value))
    return return_value


class FileStoreSegmentResult(object):
    """
    Class which holds the result data from a file segment storage
    attempt.
    """
    def __init__(self, file_id, segments_received,
                 file_result=FileStoreResultProp.NONE):
        self._file_id = file_id
        self._segments_received = segments_received
        self._file_result = file_result

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
        Segments received so far for the file

        :rtype: int
        """
        return self._segments_received

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

    def to_dict(self):
        """
        Returns a dictionary representation of the file segment results.

        :rtype: dict
        """
        dict_value = {
            FileStoreProp.ID: self._file_id,
            FileStoreProp.SEGMENTS_RECEIVED: self._segments_received
        }
        if self._file_result:
            dict_value[FileStoreProp.RESULT] = self._file_result
        return dict_value


class FileStoreManager(object):
    """
    Class which writes file segments into a backing file store.
    """

    #: "Working" subdirectory under the primary storage directory which can
    #: contain temporary files related to file storage operations.
    _STORAGE_WORK_SUBDIR = ".workdir"

    #: Key name for tracking a file hash (SHA-256 only for now)
    _FILE_HASHER = "file_hasher"

    #: Key name containing the name of the directory under which a file is
    #: stored.
    _FILE_DIR = "dir"

    #: Key name containing the full path to a file to be stored.
    _FILE_FULL_PATH = "full_path"

    def __init__(self, storage_dir):
        """
        Constructor parameters:

        :param str storage_dir: Directory under which files are stored
        """
        super(FileStoreManager, self).__init__()
        self._files = {}
        self._files_lock = threading.RLock()

        self._storage_work_dir = os.path.join(storage_dir,
                                              self._STORAGE_WORK_SUBDIR)
        if not os.path.exists(self._storage_work_dir):
            os.makedirs(self._storage_work_dir)

        logger.info("Using file store at '%s'", os.path.abspath(storage_dir))
        self._storage_dir = storage_dir
        self._purge_incomplete_files()

    def _purge_incomplete_files(self):
        """
        Purge working files for file storage operations which did not complete
        successfully.
        """
        for incomplete_file_id in os.listdir(self._storage_work_dir):
            logger.info("Purging content for incomplete file id: '%s'",
                        incomplete_file_id)
            file_path = os.path.join(self._storage_dir, incomplete_file_id)
            if os.path.exists(file_path):
                shutil.rmtree(file_path)
            os.remove(os.path.join(self._storage_work_dir, incomplete_file_id))

    def _write_file_segment(self, file_entry, segment):
        """
        Write the supplied segment to the file associated with the supplied
        file_entry.

        :param dict file_entry: Dictionary containing file information.
        :param bytes segment: Bytes of the segment to write to a file.
        """
        segments_received = file_entry[FileStoreProp.SEGMENTS_RECEIVED]
        logger.debug("Storing segment '%d' for file id: '%s'",
                     segments_received, file_entry[FileStoreProp.ID])
        with open(file_entry[self._FILE_FULL_PATH], "ab+") as file_handle:
            if segment:
                file_handle.write(segment)
                file_entry[self._FILE_HASHER].update(segment)
        file_entry[FileStoreProp.SEGMENTS_RECEIVED] = segments_received

    @staticmethod
    def _get_requested_file_result(params, file_size, file_hash):
        """
        Extract the value of the requested file result from the supplied
        params dictionary.

        :param dict params: The dictionary
        :param int file_size: A file size.
        :param str file_hash: A file hash
        :return: The requested file result. If the result is not available
            in the dictionary, 'None' is returned.
        :rtype: str
        :raises ValueError: If the file id, size, and/or hash parameter
            values are not appropriate for the requested file result
        """
        requested_file_result = params.get(FileStoreProp.RESULT)
        if requested_file_result:
            if requested_file_result == FileStoreResultProp.STORE:
                if file_size is None:
                    raise ValueError(
                        "File size must be specified for store request")
                if file_size is not None and not file_hash:
                    raise ValueError(
                        "File hash must be specified for store request")
            elif requested_file_result != FileStoreResultProp.CANCEL:
                raise ValueError(
                    "Unexpected '{}' value: '{}'".
                    format(FileStoreProp.RESULT, requested_file_result))
        return requested_file_result

    def _get_file_entry(self, file_id, file_name, segment_number):
        """
        Get file entry information for the supplied id.

        :param str file_id: Id of the file associated with the entry.
        :param str file_name: Name to store in a newly-created file entry.
        :return: Dictionary containing information for the file entry.
        :rtype: dict
        """
        file_entry = None

        with self._files_lock:
            if segment_number == 1:
                if not file_id:
                    file_id = str(uuid.uuid4()).lower()
                if file_id in self._files:
                    raise ValueError(
                        "Id of new file to store '{}' already exists".format(
                            file_id
                        )
                    )
                work_file = os.path.join(self._storage_work_dir, file_id)
                if os.path.exists(work_file):
                    raise ValueError(
                        "Work file for new file id '{}' already exists".format(
                            file_id
                        )
                    )
                with open(work_file, "w"):
                    pass
                file_dir = os.path.join(self._storage_dir, file_id)
                if not os.path.exists(file_dir):
                    os.makedirs(file_dir)
                file_entry = {
                    FileStoreProp.ID: file_id,
                    FileStoreProp.NAME: file_name,
                    FileStoreProp.SEGMENTS_RECEIVED: 0,
                    self._FILE_HASHER: hashlib.sha256(),
                    self._FILE_DIR: file_dir,
                    self._FILE_FULL_PATH: os.path.join(file_dir, file_name)
                }
                self._files[file_id] = file_entry
                logger.info("Assigning file id '%s' for '%s'", file_id,
                            file_entry[self._FILE_FULL_PATH])
            else:
                file_entry = self._files.get(file_id)
                if not file_entry:
                    raise ValueError(
                        "Unable to find file id: {}".format(file_id))

        return file_entry

    def _complete_file(self, file_entry, requested_file_result,
                       last_segment, file_size, file_hash):
        """
        Complete the storage operation for a file entry.

        :param dict file_entry: The entry of the file to complete.
        :param str requested_file_result: The desired storage result. If the
            value is
            :const:`dxlfiletransferclient.constants.FileStoreResultProp.STORE`
            but the expected size/hash does not match the stored size/hash or if
            the value is
            :const:`dxlfiletransferclient.constants.FileStoreResultProp.CANCEL`,
             the stored file contents are removed from disk.
        :param bytes last_segment: The last segment of the file to be stored.
            This may be 'None'. The last segment is not written if the
            requested_file_result is set to
            :const:`dxlfiletransferclient.constants.FileStoreResultProp.CANCEL`.
        :param int file_size: Expected size of the stored file.
        :param str file_hash: Expected SHA-256 hexstring hash of the contents of
            the stored file
        :return: The value of the requested_file_result.
        :raises ValueError: If the stored size/hash does not match the
            expected size/hash for the file.
        :rtype: str
        """
        file_id = file_entry[FileStoreProp.ID]
        file_dir = file_entry[self._FILE_DIR]
        full_file_path = file_entry[self._FILE_FULL_PATH]

        workdir_file = os.path.join(self._storage_work_dir, file_id)
        if os.path.exists(workdir_file):
            os.remove(workdir_file)

        if requested_file_result == FileStoreResultProp.STORE:
            store_error = None
            self._write_file_segment(file_entry, last_segment)
            stored_file_size = os.path.getsize(full_file_path)
            if stored_file_size != file_size:
                store_error = "Unexpected file size. Expected: '" + \
                              str(stored_file_size) + "'. Received: '" + \
                              str(file_size) + "'."
            if stored_file_size:
                stored_file_hash = file_entry[
                    self._FILE_HASHER].hexdigest()
                if stored_file_hash != file_hash:
                    store_error = "Unexpected file hash. Expected: " + \
                                  "'" + str(stored_file_hash) + \
                                  "'. Received: '" + \
                                  str(file_hash) + "'."
            if store_error:
                shutil.rmtree(file_dir)
                raise ValueError("File storage error for file '{}': {}".format(
                    file_id, store_error))
            logger.info("Stored file '%s' for id '%s'", full_file_path,
                        file_id)
            result = FileStoreResultProp.STORE
        else:
            shutil.rmtree(file_dir)
            logger.info("Canceled storage of file for id '%s'", file_id)
            result = FileStoreResultProp.CANCEL

        with self._files_lock:
            del self._files[file_id]

        return result

    def store_segment(self, message):
        """
        Process a message containing information for a file to store. If the
        request contains a file segment, the segment is written to disk.

        :param dxlclient.message.Message message: The message containing the
            file segment to process.
        :return: The result from the storage operation.
        :rtype: FileStoreSegmentResult
        :raises ValueError: If any parameters associated with the segment
            to store are invalid. For example: if the segment number for the
            message is greater than 1 but no file id is associated with the
            message.
        """
        # Extract parameters from the request. Parameters all appear in the
        # 'other_fields' element in the request. The request payload, if
        # set, represents a segment of a file to be stored.
        params = message.other_fields
        segment = message.payload

        segment_number = _get_value_as_int(params, FileStoreProp.SEGMENT_NUMBER)
        file_id = params.get(FileStoreProp.ID)
        file_name = params.get(FileStoreProp.NAME)
        if segment_number == 1:
            # File name must be specified for the first segment in a file store
            # operation. The file name can be derived from the file_id for
            # subsequent requests.
            if not file_name:
                raise ValueError(
                    "'{}' must be specified for the first file segment".format(
                        FileStoreProp.NAME
                    )
                )
        else:
            if not file_id:
                raise ValueError(
                    "'{}' must be specified for segment '{}'".format(
                        FileStoreProp.ID, segment_number)
                )

        file_size = _get_value_as_int(params, FileStoreProp.SIZE)
        file_hash = params.get(FileStoreProp.HASH_SHA256)
        requested_file_result = self._get_requested_file_result(
            params, file_size, file_hash)

        # Obtain or create a file entry for the file associated with the
        # request
        file_entry = self._get_file_entry(file_id, file_name, segment_number)

        if requested_file_result != FileStoreResultProp.CANCEL:
            segments_received = file_entry[
                FileStoreProp.SEGMENTS_RECEIVED]
            if (segments_received + 1) == segment_number:
                file_entry[FileStoreProp.SEGMENTS_RECEIVED] = \
                    segments_received + 1
            else:
                raise ValueError(
                    "Unexpected segment. Expected: '{}'. Received: '{}'".
                    format(segments_received + 1, segment_number))

        if requested_file_result:
            file_result = self._complete_file(
                file_entry, requested_file_result, segment,
                file_size, file_hash)
        else:
            self._write_file_segment(file_entry, segment)
            file_result = FileStoreResultProp.NONE

        return FileStoreSegmentResult(
            file_entry[FileStoreProp.ID],
            file_entry[FileStoreProp.SEGMENTS_RECEIVED],
            file_result
        )
