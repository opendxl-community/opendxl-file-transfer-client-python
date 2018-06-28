class HashType(object):
    """
    Constants used to indicate `hash type`.
    """
    SHA256 = "sha256"


class FileStoreProp(object):
    """
    Attributes associated with the parameters for a file store operation.
    """
    ID = "file_id"
    NAME = "name"
    SIZE = "size"

    HASHES = "hashes"
    HASH_SHA256 = "hash_sha256"

    SEGMENT_NUMBER = "segment_number"
    SEGMENTS_RECEIVED = "segments_received"
    TOTAL_SEGMENTS = "total_segments"

    RESULT = "result"


class FileStoreResultProp(object):
    """
    Attributes associated with the results for a file store operation.
    """
    CANCEL = "cancel"
    STORE = "store"
    NONE = None
