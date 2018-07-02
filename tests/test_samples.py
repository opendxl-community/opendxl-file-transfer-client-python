import hashlib
import json
import os
import random
import re
import shutil
import string
import sys
import unittest
from tempfile import mkdtemp, NamedTemporaryFile

if sys.version_info[0] > 2:
    import builtins  # pylint: disable=import-error, unused-import
else:
    import __builtin__  # pylint: disable=import-error

    builtins = __builtin__  # pylint: disable=invalid-name

# pylint: disable=wrong-import-position
from mock import patch
from dxlbootstrap.util import MessageUtils
from dxlclient.callbacks import RequestCallback
from dxlclient.client_config import DxlClientConfig
from dxlclient.client import DxlClient
from dxlclient.message import ErrorResponse, Response
from dxlclient.service import ServiceRegistrationInfo
from dxlfiletransferclient.client import FileTransferClient
from dxlfiletransferclient.store import FileStoreManager
from dxlfiletransferclient.constants import FileStoreProp, HashType


class StringMatches(object):
    def __init__(self, pattern):
        self.pattern = pattern

    def __eq__(self, other):
        return re.match(self.pattern, other, re.DOTALL)


class StringDoesNotMatch(object):
    def __init__(self, pattern):
        self.pattern = pattern

    def __eq__(self, other):
        return not re.match(self.pattern, other)


class TestFileStoreRequestCallback(RequestCallback):
    def __init__(self, dxl_client, storage_dir):
        super(TestFileStoreRequestCallback, self).__init__()
        self._store_manager = FileStoreManager(storage_dir)
        self._dxl_client = dxl_client

    def on_request(self, request):
        try:
            res = Response(request)
            result = self._store_manager.store_segment(request)
            MessageUtils.dict_to_json_payload(res, result.to_dict())
            self._dxl_client.send_response(res)
        except Exception as ex:
            err_res = ErrorResponse(request,
                                    error_message=MessageUtils.encode(str(ex)))
            self._dxl_client.send_response(err_res)


class Sample(unittest.TestCase):
    _CONFIG_FILE = "sample/dxlclient.config"
    _RANDOM_FILE_SIZE = 2 * (2 ** 20) # 2 MB

    _RANDOM_CHOICE_CHARS = string.ascii_uppercase + string.digits + \
                           string.punctuation + "\0\1\2\3"
    _SERVICE_REG_INFO_TIMEOUT = 10 # seconds

    @staticmethod
    def expected_print_output(title, detail):
        json_string = title + json.dumps(detail, sort_keys=True,
                                         separators=(".*", ": "))
        return re.sub(r"(\.\*)+", ".*",
                      re.sub(r"[{[\]}]", ".*", json_string))

    @staticmethod
    def run_sample(sample_file, sample_args):
        with open(sample_file) as f, \
                patch.object(builtins, 'print') as mock_print:
            sample_globals = {"__file__": sample_file}
            original_sys_argv = sys.argv
            try:
                if sample_args:
                    sys.argv = [sample_file] + sample_args
                exec(f.read(), sample_globals)  # pylint: disable=exec-used
            finally:
                sys.argv = original_sys_argv
        return mock_print

    def run_sample_with_service(self, sample_file, sample_args, storage_dir):
        config = DxlClientConfig.create_dxl_config_from_file(
            self._CONFIG_FILE)
        with DxlClient(config) as dxl_client:
            dxl_client.connect()
            info = ServiceRegistrationInfo(
                dxl_client, "test-file-transfer-service")
            info.add_topic(FileTransferClient._DEFAULT_FILE_SEND_TOPIC,
                           TestFileStoreRequestCallback(dxl_client,
                                                        storage_dir))
            dxl_client.register_service_sync(
                info, self._SERVICE_REG_INFO_TIMEOUT)
            mock_print = self.run_sample(sample_file, sample_args)
            dxl_client.unregister_service_sync(
                info, self._SERVICE_REG_INFO_TIMEOUT)
        return mock_print

    def create_random_file(self):
        file_hash = hashlib.sha256()
        with NamedTemporaryFile(mode="w+", delete=False) as temp_file:
            file_bytes = "".join([random.choice(self._RANDOM_CHOICE_CHARS)
                                  for _ in range(self._RANDOM_FILE_SIZE)])
            file_hash.update(file_bytes.encode())
            temp_file.write(file_bytes)
        return temp_file.name, file_hash.hexdigest()

    @staticmethod
    def get_hash_for_file(file_name):
        file_hash = hashlib.sha256()
        with open(file_name, "rb") as file_handle:
            file_data = file_handle.read()
            while file_data:
                file_hash.update(file_data)
                file_data = file_handle.read()
        return file_hash.hexdigest()

    def test_send_file_request_example(self):
        storage_dir = mkdtemp()
        source_file, source_file_hash = self.create_random_file()
        store_subdir = "subdir1/subdir2"
        expected_store_file = os.path.join(
            storage_dir, store_subdir, os.path.basename(source_file)
        )
        try:
            mock_print = self.run_sample_with_service(
                "sample/basic/basic_send_file_request_example.py",
                [source_file, store_subdir], storage_dir)
            self.assertTrue(os.path.exists(expected_store_file))
            self.assertEqual(source_file_hash,
                             self.get_hash_for_file(expected_store_file))
            mock_print.assert_any_call(
                StringMatches(
                    self.expected_print_output(
                        "\nResponse:",
                        {
                            FileStoreProp.HASHES: {
                                HashType.SHA256: source_file_hash
                            },
                            FileStoreProp.SIZE: os.path.getsize(source_file)
                        }
                    )
                )
            )
            mock_print.assert_any_call(StringDoesNotMatch(
                "Error invoking request"))
        finally:
            os.remove(source_file)
            shutil.rmtree(storage_dir)
