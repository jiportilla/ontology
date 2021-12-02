#!/usr/bin/env python
# -*- coding: UTF-8 -*-


from pandas import DataFrame

from base import BaseObject
from datamongo.core.dmo import BaseMongoClient


class TextQueryAPI(BaseObject):
    """ Perform a Text Query on a Cendant Collection
    """

    _records = None

    def __init__(self,
                 mongo_client: BaseMongoClient,
                 is_debug: bool = False):
        """
        Created:
            16-Oct-2019
            craig.trim@ibm.com
            *   https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1122#issuecomment-15340543
        :param mongo_client:
            the instantiated connection to mongoDB
        :param is_debug:
            True        write debug statements to the console
        """
        BaseObject.__init__(self, __name__)

        self._is_debug = is_debug
        self._mongo_client = mongo_client

    def source_collection(self,
                          name: str,
                          term: str,
                          window_size: int = 5) -> DataFrame:
        from datamongo.text.svc import SearchSourceCollection

        return SearchSourceCollection(name=name,
                                      term=term,
                                      window_size=window_size,
                                      mongo_client=self._mongo_client,
                                      is_debug=self._is_debug).process()


def main(collection_name, term, window_size):
    from tabulate import tabulate
    from datamongo import BaseMongoClient

    api = TextQueryAPI(is_debug=False, mongo_client=BaseMongoClient())
    df = api.source_collection(collection_name, term, window_size=int(window_size))
    print(tabulate(df,
                   headers='keys',
                   tablefmt='psql'))


if __name__ == "__main__":
    import plac

    plac.call(main)
