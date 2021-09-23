import hashlib


def get_file_hash(filepath) -> str:
    """
    Returns SHA256 hash of the file.
    """
    with open(filepath, 'rb') as f:
        return hashlib.sha256(f.read()).hexdigest()
