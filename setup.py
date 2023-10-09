from setuptools import find_packages, setup

setup(
    name="NoAlchemy",
    version="0.1.0",
    description="A MongoDB Object-Document Mapper (ODM) based on SQLAlchemy.",
    author="grantUser",
    url="https://github.com/grantUser/NoAlchemy",
    packages=find_packages(),
    install_requires=[
        "pymongo",
        "mongomock",
    ],
)
