#!/usr/bin/env python
# -*- coding: UTF-8 -*-


from datetime import datetime

from base import BaseObject


class CollectionNameGenerator(BaseObject):
    """ GitHub Ingestion Orchestrator """

    def __init__(self,
                 repo_name: str,
                 collection_type: str,
                 is_debug: bool = False):
        """
        Created:
            6-Dec-2019
            craig.trim@ibm.com
            *   https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1532
        :param repo_name:
            name of the Github repo
            e.g.,   'unstructured-analytics'
        :param collection_type:
            the type of collection to generate
            e.g.,   'src' or 'tag'
        """
        BaseObject.__init__(self, __name__)
        self._is_debug = is_debug
        self._repo_name = repo_name
        self._collection_type = collection_type

    def _repo_abbrev(self) -> str:
        if self._repo_name.lower() == "unstructured-analytics":
            return "una"
        if self._repo_name.lower() == "cdo-workforce-transformation":
            return "cdo"

        self.logger.warning(f"Unrecognized Repository: "
                            f"{self._repo_name}")
        return self._repo_name.replace('-', '_')

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

    def process(self) -> str:
        collection_name = f"github-{self._repo_abbrev()}_{self._collection_type}_{self._date()}"

        if self._is_debug:
            self.logger.debug('\n'.join([
                "Generated Collection Name",
                f"\tName: {collection_name}",
                f"\tRepo: {self._repo_name}"]))

        return collection_name
