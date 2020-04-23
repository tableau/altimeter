from typing import Optional, Tuple

from altimeter.core.artifact_io.exceptions import InvalidS3URIException

S3_URI_PREFIX = "s3://"


def is_s3_uri(path: str) -> bool:
    if path.startswith(S3_URI_PREFIX):
        return True
    return False


def parse_s3_uri(uri: str) -> Tuple[str, Optional[str]]:
    """Parse an s3 uri (s3://bucket/key/path) into bucket and key parts

    Args:
        uri: s3 uri (s3://bucket/key/path)

    Returns:
        Tuple of (bucket, key)

    Raises:
        :class:`InvalidS3URIException` if the uri
        argument is not a valid S3 URI.
    """
    if not is_s3_uri(uri):
        raise InvalidS3URIException(f"S3 URIs should begin with '{S3_URI_PREFIX}'")
    uri = uri.rstrip("/")
    parts = uri[len(S3_URI_PREFIX) :].split("/")
    if len(parts) < 1:
        raise InvalidS3URIException(f"{uri} missing bucket portion")
    bucket = parts[0]
    if not bucket:
        raise InvalidS3URIException(f"Bad bucket portion in uri {uri}")
    key = None
    if len(parts) > 1:
        key_parts = [part.rstrip("/ ") for part in parts[1:]]
        if not all(key_parts):
            raise InvalidS3URIException(f"Bad key portion in uri {uri}")
        key = "/".join(key_parts)
    return bucket, key
