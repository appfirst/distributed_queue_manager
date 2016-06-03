"""
Methods used for handling decoding / encoding various strings.
"""


def cast_bytes(data, encoding='utf8'):
    """
    Cast str, int, float to bytes.
    """
    if isinstance(data, str) is True:
        return data.encode(encoding)

    elif isinstance(data, int) is True:
        return str(data).encode(encoding)

    elif isinstance(data, float) is True:
        return str(data).encode(encoding)

    elif isinstance(data, bytes) is True:
        return data

    elif data is None:
        return None

    else:
        raise TypeError("Expected unicode or bytes, got %r" % data)


def cast_string(data, encoding='utf8'):
    """
    Cast bytes, int, float to string (unicode as default).
    """
    if isinstance(data, bytes) is True:
        return data.decode(encoding)

    elif isinstance(data, int) is True:
        return str(data).encode(encoding)

    elif isinstance(data, float) is True:
        return str(data).encode(encoding)

    elif isinstance(data, str) is True:
        return data

    elif data is None:
        return None

    else:
        raise TypeError("Expected unicode or bytes, got %r" % data)


def cast_list_of_strings(bytes_list):
    """
    Cast a list of bytes to strings
    """
    return [cast_string(byte) for byte in bytes_list]


def cast_list_of_bytes(string_list):
    """
    Cast a list of strings to bytes
    """
    return [cast_bytes(string) for string in string_list]