# !/usr/bin/env python
# -*- coding: UTF-8 -*-

from base import BaseObject
from cendalytics.skills.core.bp import SkillsReportAPI
from datamongo import CendantCollectionRegistry


class TagSearchTutorial(BaseObject):
    """

    """

    def __init__(self,
                 is_debug: bool = False):
        """
        Created:
            13-Nov-2019
            craig.trim@ibm.com
        """
        BaseObject.__init__(self, __name__)
        self._is_debug = is_debug
        self._api = SkillsReportAPI(is_debug=is_debug)
        self._collection_registry = CendantCollectionRegistry(is_debug=self._is_debug)

    def single_tag_single_division(self) -> None:
        """
        Purpose:
            Find Ansible Skills in GBS
        :return:
        """
        collection_name = self._collection_registry.by_date("20191025").supply().tag()
        searcher = self._api.search(collection_name=collection_name)

        searcher.tags(tags=["Ansible"], div_field="GBS").process()


def main():
    TagSearchTutorial(is_debug=True).single_tag_single_division()


if __name__ == "__main__":
    main()
