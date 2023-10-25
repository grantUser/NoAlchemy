from setuptools import find_packages, setup
from noalchemy.version import get_version_string

setup(
    name="NoAlchemy",
    version=get_version_string(),
    author="grantUser",
    description="A MongoDB Object-Document Mapper (ODM) based on SQLAlchemy.",
    url="https://github.com/grantUser/NoAlchemy",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    packages=find_packages(),
    install_requires=[
        "pymongo",
        "mongomock",
        "pytest"
    ],
    python_requires=">=3.8, <4",
    keywords=["MongoDB", "Object-Document Mapper", "ODM", "SQLAlchemy"],
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Topic :: Database",
        "Topic :: Software Development :: Libraries :: Python Modules",
    ],
    license="MIT",
    maintainer="grantUser"
)
