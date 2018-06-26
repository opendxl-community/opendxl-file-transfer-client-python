class FileStoreProp(object):
    """
    Attributes associated with the parameters for a file store operation.
    """
    ID = "file_id"
    SIZE = "size"
    HASHES = "hashes"

    SEGMENTS_RECEIVED = "segments_received"
    TOTAL_SEGMENTS = "total_segments"


class HashType(object):
    """
    Constants used to indicate `hash type`.
    """
    SHA256 = "sha256"
