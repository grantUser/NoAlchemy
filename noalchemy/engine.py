import re
from urllib.parse import parse_qs, urlencode, urlparse, urlunparse
from pymongo import InsertOne, UpdateOne, DeleteOne

class _engine:
    def __init__(self, url: str, pool: bool = True, mock: bool = False) -> None:
        self.url = url
        self.pool = pool
        self.mock = mock
        self.client = None
        self.database = None

        self.__post_init__()

    def __post_init__(self) -> None:
        if infos := self._parse_url():
            self.client = self._MongoClient()

            if database := infos.get("database", False):
                self.database = self.client[database]

    def _parse_url(self) -> None:
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

        match = pattern.match(self.url)
        if match:
            infos = match.groupdict()

            if name := infos.get("name", False):
                if name == "mongodb" or name == "mongodb+srv":
                    if infos.get("database", False):
                        return infos

            raise Exception("Invalid URL detected")

    def _MongoClient(self):
        from pymongo import MongoClient

        if self.mock:
            from mongomock import MongoClient

        parsed_url = urlparse(self.url)
        query_parameters = parse_qs(parsed_url.query)
        parsed_url = urlparse(self.url)
        query_parameters = parse_qs(parsed_url.query)
        query_parameters["appName"] = "NoAlchemy"

        if not self.pool:
            query_parameters["directConnection"] = "true"

        new_query_string = urlencode(query_parameters, doseq=True)
        self.url = urlunparse(
            (
                parsed_url.scheme,
                parsed_url.netloc,
                parsed_url.path,
                parsed_url.params,
                new_query_string,
                parsed_url.fragment,
            )
        )
        return MongoClient(self.url)
