from setuptools import find_packages, setup
from noalchemy.version import get_version_string 

setup(
    name="NoAlchemy",
    version=get_version_string(),
    description="A MongoDB Object-Document Mapper (ODM) based on SQLAlchemy.",
    author="grantUser",
    url="https://github.com/grantUser/NoAlchemy",
    packages=find_packages(),
    install_requires=[
        "pymongo",
        "mongomock",
    ],
)
