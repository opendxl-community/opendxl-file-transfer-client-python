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

_PATH_NAME_SEPARATORS = (".", "\\", "/")


def _contains_path_name_separators(value):
    """
    Determine if the supplied value contains any characters which
    could represent a path name separator.

    :param str value: Value to check for path name separators
    :return: True if the value contains possible path name separators, False if
        not.
    :rtype: bool
    """
    contains = False
    if value:
        for pathname_sep_chars in _PATH_NAME_SEPARATORS:
            if pathname_sep_chars in value:
                contains = True
                break
    return contains


def _get_value_as_int(dict_obj, key):
    """
    Return the value associated with a key in a dictionary, converted to an
    int.

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

    #: Default location within the storage directory to place the working
    #: directory
    _DEFAULT_WORKING_SUBDIR = ".workdir"

    #: Base file name for temporary files written in a file's working directory
    _WORKING_BASE_FILE_NAME = "file"

    #: Key name for tracking a file hash (SHA-256 only for now)
    _FILE_HASHER = "file_hasher"

    #: Key name containing the name of the working directory under which a file
    #: stored.
    _FILE_WORKING_DIR = "work_dir"

    def __init__(self, storage_dir, working_dir=None):
        """
        Constructor parameters:

        :param str storage_dir: Directory under which files are stored. If
            the directory does not already exist, an attempt will be made
            to create it.
        :param str working_dir: Working directory under which files (or
            segments of files) may be stored in the process of being
            transferred to the `storage_dir`. If not specified, this defaults
            to ".workdir" under the value specified for the `storage_dir`
            parameter.
        :raises PermissionError: If the `storage_dir` does not exist and cannot
            be created due to insufficient permissions.
        """
        super(FileStoreManager, self).__init__()
        self._files = {}
        self._files_lock = threading.RLock()

        self._storage_dir = os.path.abspath(storage_dir)
        if not os.path.exists(self._storage_dir):
            os.makedirs(self._storage_dir)
        logger.info("Using storage dir: %s", storage_dir)

        self._working_dir = os.path.abspath(working_dir) \
            if working_dir else os.path.join(self._storage_dir,
                                             self._DEFAULT_WORKING_SUBDIR)
        if not os.path.exists(self._working_dir):
            os.makedirs(self._working_dir)
        logger.info("Using working dir: %s", self._working_dir)

        self._purge_incomplete_files()

    def _get_working_file_dir(self, file_id):
        """
        Get the working file directory for the supplied file_id.

        :param str file_id: Id to get the working file directory for.
        :return: The working file directory.
        :rtype: str
        """
        return os.path.join(self._working_dir, file_id)

    def _get_working_file_name(self, file_id):
        """
        Get the working file name for the supplied file_id.

        :param str file_id: Id to get the working file name for.
        :return: The working file name.
        :rtype: str
        """
        return os.path.join(self._get_working_file_dir(file_id),
                            self._WORKING_BASE_FILE_NAME)

    def _purge_incomplete_files(self):
        """
        Purge working files for file storage operations which did not complete
        successfully.
        """
        for incomplete_file_id in os.listdir(self._working_dir):
            logger.info("Purging content for incomplete file id: '%s'",
                        incomplete_file_id)
            file_work_dir = self._get_working_file_dir(incomplete_file_id)
            shutil.rmtree(file_work_dir)

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
        with open(self._get_working_file_name(file_entry[FileStoreProp.ID]),
                  "ab+") as file_handle:
            if segment:
                file_handle.write(segment)
                file_entry[self._FILE_HASHER].update(segment)
        file_entry[FileStoreProp.SEGMENTS_RECEIVED] = segments_received

    @staticmethod
    def _get_requested_file_result(params, file_name, file_size, file_hash):
        """
        Extract the value of the requested file result from the supplied
        params dictionary.

        :param dict params: The dictionary
        :param str file_name: File name under the storage file directory
            in which to store the file.
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
                if file_name is None:
                    raise ValueError(
                        "File name must be specified for store request"
                    )
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

    def _get_file_entry(self, file_id):
        """
        Get file entry information for the supplied id.

        :param str file_id: Id of the file associated with the entry.
        :rtype: dict
        """
        with self._files_lock:
            if not file_id:
                file_id = str(uuid.uuid4()).lower()
            file_entry = self._files.get(file_id)
            if not file_entry:
                if file_id in self._files:
                    raise ValueError(
                        "Id of new file to store '{}' already exists".format(
                            file_id
                        )
                    )
                file_working_dir = self._get_working_file_dir(file_id)
                if os.path.exists(file_working_dir):
                    raise ValueError(
                        "Work directory for new file id '{}' already exists".
                        format(file_id)
                    )
                os.makedirs(file_working_dir)
                file_entry = {
                    FileStoreProp.ID: file_id,
                    FileStoreProp.SEGMENTS_RECEIVED: 0,
                    self._FILE_HASHER: hashlib.sha256(),
                    self._FILE_WORKING_DIR: file_working_dir,
                }
                self._files[file_id] = file_entry
                logger.info("Assigning file id '%s' for '%s'", file_id,
                            file_entry[self._FILE_WORKING_DIR])
        return file_entry

    def _validate_file(self, file_entry, file_size, file_hash):
        """
        Validate that a file was stored correctly.

        :param dict file_entry: The entry of the file to complete.
        :param int file_size: Expected size of the stored file.
        :param str file_hash: Expected SHA-256 hexstring hash of the contents
            of the stored file
        """
        file_id = file_entry[FileStoreProp.ID]
        file_working_name = self._get_working_file_name(file_id)

        store_error = None
        stored_file_size = os.path.getsize(file_working_name)
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
            raise ValueError(
                "File storage error for file '{}': {}".format(
                    file_id, store_error))

    def _complete_file(self, file_entry, requested_file_result, last_segment,
                       file_name, file_size, file_hash):
        """
        Complete the storage operation for a file entry.

        :param dict file_entry: The entry of the file to complete.
        :param str requested_file_result: The desired storage result. If the
            value is
            :const:`dxlfiletransferclient.constants.FileStoreResultProp.STORE`
            but the expected size/hash does not match the stored size/hash or
            if the value is
            :const:`dxlfiletransferclient.constants.FileStoreResultProp.CANCEL`,
             the stored file contents are removed from disk.
        :param bytes last_segment: The last segment of the file to be stored.
            This may be 'None'. The last segment is not written if the
            requested_file_result is set to
            :const:`dxlfiletransferclient.constants.FileStoreResultProp.CANCEL`.
        :param str file_name: File name under the storage file directory
            in which to store the file.
        :param int file_size: Expected size of the stored file.
        :param str file_hash: Expected SHA-256 hexstring hash of the contents
            of the stored file
        :return: The value of the requested_file_result.
        :raises ValueError: If the stored size/hash does not match the
            expected size/hash for the file.
        :rtype: str
        """
        file_id = file_entry[FileStoreProp.ID]
        file_working_dir = self._get_working_file_dir(file_id)
        file_working_name = self._get_working_file_name(file_id)

        try:
            if requested_file_result == FileStoreResultProp.STORE:
                self._write_file_segment(file_entry, last_segment)
                self._validate_file(file_entry, file_size, file_hash)

                file_dir = os.path.dirname(file_name)
                if not os.path.exists(file_dir):
                    os.makedirs(file_dir)
                elif os.path.exists(file_name):
                    os.remove(file_name)
                os.rename(file_working_name, file_name)

                logger.info("Stored file '%s' for id '%s'", file_name, file_id)
                result = FileStoreResultProp.STORE
            else:
                logger.info("Canceled storage of file for id '%s'", file_id)
                result = FileStoreResultProp.CANCEL
        finally:
            shutil.rmtree(file_working_dir)
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

        segment_number = _get_value_as_int(params,
                                           FileStoreProp.SEGMENT_NUMBER)

        file_id = params.get(FileStoreProp.ID)
        if _contains_path_name_separators(file_id):
            raise ValueError(
                "File id cannot contain path name separators: '{}'".format(
                    file_id))

        file_name = params.get(FileStoreProp.NAME)
        if file_name:
            abs_file_name = os.path.abspath(os.path.join(
                self._storage_dir, file_name))
            if not abs_file_name.startswith(self._storage_dir + os.sep):
                raise ValueError(
                    "File name cannot be outside of storage directory: '{}'".
                    format(file_name))
            if abs_file_name.startswith(self._working_dir + os.sep):
                raise ValueError(
                    "File name cannot be in working directory: '{}'".format(
                        file_name))

            file_name = abs_file_name

        file_size = _get_value_as_int(params, FileStoreProp.SIZE)
        file_hash = params.get(FileStoreProp.HASH_SHA256)
        requested_file_result = self._get_requested_file_result(
            params, file_name, file_size, file_hash)

        # Obtain or create a file entry for the file associated with the
        # request
        file_entry = self._get_file_entry(file_id)

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
                file_name, file_size, file_hash)
        else:
            self._write_file_segment(file_entry, segment)
            file_result = FileStoreResultProp.NONE

        return FileStoreSegmentResult(
            file_entry[FileStoreProp.ID],
            file_entry[FileStoreProp.SEGMENTS_RECEIVED],
            file_result
        )
