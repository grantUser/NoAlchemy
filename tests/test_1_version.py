# Import necessary modules
from noalchemy import __version__, version


def test_version_variables():
    assert version == __version__
