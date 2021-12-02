#!/usr/bin/env python
# -*- coding: UTF-8 -*-


from datetime import datetime

from base import BaseObject


class CollectionNameGenerator(BaseObject):
    """ GitHub Ingestion Orchestrator """

    def __init__(self,
                 is_debug: bool = False):
        """
        Created:
            24-Dec-2019
            craig.trim@ibm.com
            *   based on 'collection-name-generator' from github analysis
            *   collection-type parameter introduced for
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1642#issuecomment-16802836
        """
        BaseObject.__init__(self, __name__)
        self._is_debug = is_debug

    @staticmethod
    def _date() -> str:
        def _current_day() -> str:
            x = datetime.now().day
            if x < 10:
                return f"0{x}"
            return str(x)

        def _current_month() -> str:
            x = datetime.now().month
            if x < 10:
                return f"0{x}"
            return str(x)

        current_year = datetime.now().year

        return f"{current_year}{_current_month()}{_current_day()}"

    def process(self,
                collection_type:str) -> str:
        collection_name = f"parse_unstrut-{collection_type}_{self._date()}"

        if self._is_debug:
            self.logger.debug('\n'.join([
                "Generated Collection Name",
                f"\tName: {collection_name}"]))

        return collection_name
