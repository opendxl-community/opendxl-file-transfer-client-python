Overview
========

The File Transfer DXL Python client library provides the ability to transfer
files via the
`Data Exchange Layer <http://www.mcafee.com/us/solutions/data-exchange-layer.aspx>`_
(DXL) fabric.

OpenDXL brokers are configured by default to limit the maximum size of a message
to 1 MB. The File Transfer DXL Python client allows the contents of a file to be
transferred in segments small enough to fit into a DXL message.

This client can be used in conjunction with the
`File Transfer DXL Service <https://github.com/opendxl-community/opendxl-file-transfer-service-python>`_,
an application which registers a service with the DXL fabric and a request
callback which can store files sent to it.
