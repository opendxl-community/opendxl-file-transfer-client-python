Basic Send File Request Example
===============================

The sample uses a client application wrapper to send a file to a File Transfer
service via the DXL fabric. The progress and result of the file storage
operation are displayed to the console.

Prerequisites
*************

* The samples configuration step has been completed (see :doc:`sampleconfig`)
* The File Transfer DXL service is running (see
  `File Transfer DXL Service <https://github.com/opendxl-community/opendxl-file-transfer-service-python>`__).

Running
*******

To run this sample execute the
``sample/basic/basic_send_file_request_example.py`` script with the path to the
file to be sent to the service as a parameter. For example, to send a file named
``C:\test.exe`` to the service, you could run the sample as follows:

    .. parsed-literal::

        python sample/basic/basic_send_file_request_example.py C:\\test.exe

As the file is being sent, a "Percent complete" indicator -- moving from 0% to
100% -- should be updated:

    .. code-block:: shell

        Percent complete: 5%

After the file has been uploaded completely, the response from the service and
some summary information for the file store operation should be printed out. For
example:

    .. code-block:: shell

        Percent complete: 100%
        Response:
        {
            "file_id": "7b89f71d-f348-45ee-aef3-4ac2555e92f8",
            "hashes": {
                "sha256": "a2e52129a28feec1ee3f22f5aaf9bdecbb02d51af6da408ace0a2ac2e0365c8b"
            },
            "size": 89579672
        }
        Elapsed time (ms): 89546.39649391174

The service stores files under the directory configured for the `storageDir`
setting in the service configuration file. For example, if this setting were
specified as ``C:\\dxl-file-store`` and the base name of the file supplied as a
parameter to the ``basic_send_file_request_example.py`` script were
``test.exe``, the file would be stored at the following location:

    .. parsed-literal::

        C:\\dxl-file-store\\test.exe

If a second parameter is passed to the example when run, the extra parameter
is used as the name of the subdirectory under which the file should be stored.
For example, the following command could be run:

    .. parsed-literal::

        python sample/basic/basic_send_file_request_example.py C:\\test.exe storesub1/storesub2

Assuming the storage directory setting on the server were specified as
``C:\\dxl-file-store``, the file would be stored at the following location:

    .. parsed-literal::

        C:\\dxl-file-store\\storesub1\\storesub2\\test.exe

Details
*******

The majority of the sample code is shown below:

    .. code-block:: python

        # Create the client
        with DxlClient(config) as dxl_client:

            # Connect to the fabric
            dxl_client.connect()

            logger.info("Connected to DXL fabric.")

            # Create client wrapper
            client = FileTransferClient(dxl_client)

            start = time.time()

            # Invoke the send file request method to store the file on the server
            resp = client.send_file_request(
                STORE_FILE_NAME,
                file_name_on_server=os.path.join(
                    STORE_FILE_DIR, os.path.basename(STORE_FILE_NAME)),
                max_segment_size=MAX_SEGMENT_SIZE,
                callback=update_progress)

            # Print out the response (convert dictionary to JSON for pretty printing)
            print("\nResponse:\n{}".format(
                MessageUtils.dict_to_json(resp.to_dict(), pretty_print=True)))

            print("Elapsed time (ms): {}".format((time.time() - start) * 1000))


After connecting to the DXL fabric, a `FileTransferClient` is created.

The next step is to invoke the `send_file_request` method on the
`FileTransferClient` instance. This call sends the file contents to the DXL
fabric. As the File Transfer DXL service receives DXL ``request messages`` with
the file segments, the segments are reassembled into a single file which is
stored to the file system.

Assuming the file store operation is successful, the response from the service
is printed to the console output. The response contains a ``sha256`` hash and
``size`` of the file bytes which were stored on the server.
