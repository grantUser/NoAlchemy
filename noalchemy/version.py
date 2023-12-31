from typing import Tuple, Union

version_tuple: Tuple[Union[int, str], ...] = (0, 3, 1)


def get_version_string() -> str:
    if isinstance(version_tuple[-1], str):
        return ".".join(map(str, version_tuple[:-1])) + version_tuple[-1]
    return ".".join(map(str, version_tuple))


__version__: str = get_version_string()
version = __version__
