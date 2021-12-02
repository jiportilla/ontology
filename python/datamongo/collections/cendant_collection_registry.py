#!/usr/bin/env python
# -*- coding: UTF-8 -*-


from base import BaseObject

from ..core.dmo import BaseMongoClient


class CendantCollectionRegistry(BaseObject):
    """ Service to Provide access to the Cendant Collection Registry """

    class CollectionNamesLoader(object):
        def __init__(self,
                     mongo_client: BaseMongoClient):
            self._client = mongo_client

        def names(self) -> list:
            return self._client.client.cendant.list_collection_names()

    def __init__(self,
                 collection_names_loader=None,
                 is_debug: bool = True):
        """
        Created:
            13-Nov-2019
            craig.trim@ibm.com
            *   https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1342
        """
        BaseObject.__init__(self, __name__)
        self._is_debug = is_debug
        if not collection_names_loader:
            collection_names_loader = CendantCollectionRegistry.CollectionNamesLoader(BaseMongoClient())
        self._config = self._load(collection_names_loader)

    def _load(self, collection_names_loader) -> dict:
        template = {
            "src": "",
            "tag": "",
            "xdm": ""
        }
        config = {
            "demand": {
                "latest": dict(template)
            },
            "learning": {
                "latest": dict(template)
            },
            "supply": {
                "latest": dict(template)
            }
        }
        for collection in collection_names_loader.names():
            for key in config.keys():
                fragments = collection.split('_')
                if len(fragments) != 3:
                    continue
                category, kind, date = tuple(fragments)
                if category == key:
                    if date not in config[key]:
                        config[category][date] = dict(template)
                    config[category][date][kind] = collection
                    if collection > config[category]['latest'][kind]:
                        config[category]['latest'][kind] = collection

        return config

    def _by_name(self,
                 facade,
                 name: str):
        name = name.lower().strip()
        if name == "supply":
            return facade.supply()
        elif name == "demand":
            return facade.demand()
        elif name == "learning":
            return facade.learning()

        raise NotImplementedError(f"Name Not Recognized ("
                                  f"name={name})")

    @staticmethod
    def by_type(a_dict: dict):
        class Facade(object):
            @staticmethod
            def src() -> str:
                return a_dict["src"]

            @staticmethod
            def tag() -> str:
                return a_dict["tag"]

            @staticmethod
            def xdm() -> str:
                return a_dict["xdm"]

            @staticmethod
            def all() -> dict:
                return a_dict

        return Facade()

    def list(self):
        def _cleanse(keys: list):
            return sorted([x for x in keys if x != "latest"])

        class Facade(object):

            @classmethod
            def supply(cls):
                return _cleanse(self._config["supply"].keys())

            @classmethod
            def demand(cls):
                return _cleanse(self._config["demand"].keys())

            @classmethod
            def learning(cls):
                return _cleanse(self._config["learning"].keys())

            @classmethod
            def by_name(cls,
                        name: str):
                return self._by_name(cls, name)

        return Facade()

    def latest(self):
        def _latest(a_name: str):
            return self._config[a_name]["latest"]

        class Facade(object):
            @classmethod
            def supply(cls):
                return self.by_type(_latest("supply"))

            @classmethod
            def demand(cls):
                return self.by_type(_latest("demand"))

            @classmethod
            def learning(cls):
                return self.by_type(_latest("learning"))

            @classmethod
            def by_name(cls,
                        name: str):
                return self._by_name(cls, name)

        return Facade()

    def by_date(self,
                a_date: str):
        def _by_date(a_name: str):
            if a_date not in self._config[a_name]:
                raise ValueError(f"Date Not Found ("
                                 f"name={a_name}, "
                                 f"date={a_date})")

            return self._config[a_name][a_date]

        class Facade(object):
            @staticmethod
            def supply():
                return self.by_type(_by_date("supply"))

            @staticmethod
            def demand():
                return self.by_type(_by_date("demand"))

            @staticmethod
            def learning():
                return self.by_type(_by_date("learning"))

            @classmethod
            def by_name(cls,
                        name: str):
                return self._by_name(cls, name)

        return Facade()


if __name__ == "__main__":
    print(CendantCollectionRegistry().list().supply())
    print(CendantCollectionRegistry().list().demand())
    print(CendantCollectionRegistry().list().learning())
    print(CendantCollectionRegistry().list().by_name("learning"))

    print(CendantCollectionRegistry().latest().supply().tag())
    print(CendantCollectionRegistry().latest().demand().all())
    print(CendantCollectionRegistry().latest().learning().all())
    print(CendantCollectionRegistry().latest().by_name("supply").all())

    print(CendantCollectionRegistry().by_date("20190913").by_name("supply").all())
