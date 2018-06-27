from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
import os
import sys
import time

from dxlbootstrap.util import MessageUtils
from dxlclient.client_config import DxlClientConfig
from dxlclient.client import DxlClient
from dxlfiletransferclient import FileTransferClient, FileStoreProp

root_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(root_dir + "/../..")
sys.path.append(root_dir + "/..")

# Import common logging and configuration
from common import *

# Configure local logger
logging.getLogger().setLevel(logging.ERROR)
logger = logging.getLogger(__name__)

# Create DXL configuration from file
config = DxlClientConfig.create_dxl_config_from_file(CONFIG_FILE)

# Extract the name of the file to upload from a command line argument
STORE_FILE_NAME = None
if len(sys.argv) == 2:
    STORE_FILE_NAME = sys.argv[1]
else:
    print("Name of file to store must be specified as an argument")
    exit(1)

# Send the file contents in 50 KB segments. The default maximum size
# for a DXL broker message is 1 MB.
MAX_SEGMENT_SIZE = 50 * (2 ** 10)


# As the response is received from the service for each file segment
# which is transmitted, update a percentage complete counter to show
# progress
def update_progress(segment_response):
    total_segments = segment_response[FileStoreProp.TOTAL_SEGMENTS]
    segment_number = segment_response[FileStoreProp.SEGMENTS_RECEIVED]
    sys.stdout.write("\rPercent complete: {}%".format(
        int((segment_number / total_segments) * 100)
        if total_segments else 100))
    sys.stdout.flush()

# Create the client
with DxlClient(config) as dxl_client:

    # Connect to the fabric
    dxl_client.connect()

    logger.info("Connected to DXL fabric.")

    # Create client wrapper
    client = FileTransferClient(dxl_client)

    start = time.time()

    # Invoke the store file method to store the file on the server
    resp_dict = client.store_file(STORE_FILE_NAME,
                                  max_segment_size=MAX_SEGMENT_SIZE,
                                  callback=update_progress)

    # Print out the response (convert dictionary to JSON for pretty printing)
    print("\nResponse:\n{}".format(
        MessageUtils.dict_to_json(resp_dict, pretty_print=True)))

    print("Elapsed time (ms): {}".format((time.time() - start) * 1000))
