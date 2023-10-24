from typing import Tuple, Union

version_tuple: Tuple[Union[int, str], ...] = (0, 1, 3)


def get_version_string() -> str:
    if isinstance(version_tuple[-1], str):
        return ".".join(map(str, version_tuple[:-1])) + version_tuple[-1]
    return ".".join(map(str, version_tuple))


__version__: str = get_version_string()
version = __version__
__prod__ = True