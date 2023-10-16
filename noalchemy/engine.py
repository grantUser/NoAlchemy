import re


class _engine:
    def __init__(self, url: str, mock: bool = False) -> None:
        self.url = url
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
                if name == "mongodb":
                    if infos.get("database", False):
                        return infos

            raise Exception("Invalid URL detected")

    def _MongoClient(self):
        from pymongo import MongoClient

        if self.mock:
            from mongomock import MongoClient

        return MongoClient(self.url)
