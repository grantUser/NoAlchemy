import contextlib
import re
from urllib.parse import parse_qs, urlencode, urlparse, urlunparse

from pymongo import timeout as pymongo_timeout


class _engine:
    def __init__(self, *args, **kwds) -> None:
        self.__url = kwds.get("url", None)
        self.__username = kwds.get("username", None)
        self.__password = kwds.get("password", None)
        self.__host = kwds.get("host", None)
        self.__port = kwds.get("port", None)
        self.pool = kwds.get("pool", None)
        self.mock = kwds.get("mock", None)
        self.database = kwds.get("database", None)
        self.safe = kwds.get("safe", None)
        self.timezone = kwds.get("timezone", None)
        self.cache_size = kwds.get("cache_size", 0)
        self.tz_aware = kwds.get("cache_size", False)
        self.autocommit = kwds.get("autocommit", False)

        self.client = None

        self.__post_init__(*args, **kwds)

    def __post_init__(self, *args, **kwds) -> None:
        if args:
            self.__url = args[0]
        elif (
            self.__username
            and self.__password
            and self.__host
            and self.__port
            and self.database
        ):
            self.__url = f"mongodb://{self.__username}:{self.__password}@{self.__host}:{self.__port}/{self.database}"
        elif self.__host and self.__port and self.database:
            self.__url = f"mongodb://{self.__host}:{self.__port}/{self.database}"

        infos = self._parse_url()
        if infos:
            self.client = self._MongoClient()
            if database := infos.get("database"):
                self.database = self.client[database]

    def _parse_url(self) -> dict:
        pattern = re.compile(
            r"""
                (?P<name>[\w\+]+)://
                (?:
                    (?P<username>[^:/]*)
                    (?::(?P<password>[^@]*))?
                @)?
                (?:
                    (?:
                        \[(?P<ipv6host>[^/\?]+)\] |
                        (?P<ipv4host>[^/:\?]+)
                    )?
                    (?::(?P<port>[^/\?]*))?
                )?
                (?:/(?P<database>[^\?]*))?
                (?:\?(?P<query>.*))?
            """,
            re.X,
        )
        match = pattern.match(self.__url)
        if match:
            infos = match.groupdict()
            if infos.get("name") in ["mongodb", "mongodb+srv"] and infos.get(
                "database"
            ):
                return infos
            raise Exception("Invalid URL detected")

    def _MongoClient(self):
        from pymongo import MongoClient

        if self.mock:
            from mongomock import MongoClient

        parsed_url = urlparse(self.__url)
        query_parameters = parse_qs(parsed_url.query)
        query_parameters["appName"] = "NoAlchemy"

        if not self.pool:
            query_parameters["directConnection"] = "true"

        new_query_string = urlencode(query_parameters, doseq=True)
        return MongoClient(
            urlunparse(
                (
                    parsed_url.scheme,
                    parsed_url.netloc,
                    parsed_url.path,
                    parsed_url.params,
                    new_query_string,
                    parsed_url.fragment,
                )
            )
        )

    def isConnected(self, timeout: int = 2):
        with contextlib.suppress(Exception):
            with pymongo_timeout(timeout):
                if self.client.admin.command("ping")["ok"] == 1:
                    return True

        return False
