# !/usr/bin/env python
# -*- coding: UTF-8 -*-


from base import BaseObject
from cendalytics.skills.core.svc import PerformKeyFieldSearch


class SkillsSearchOnKeyField(BaseObject):
    """ Facade: Service that Performs a Key Field Search on normalized text in MongoDB """

    def __init__(self,
                 key_field: str,
                 collection_name: str,
                 is_debug: bool = False):
        """
        Created:
            8-Nov-2019
            craig.trim@ibm.com
            *   refactored out of skills-report-api
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1293
        :param collection_name:
            the name of the collection to perform the skills search in
        :param key_field:
            the key field to restrict the search on
            if None     search all key fields
            Note:       the meaning of the key-field varies by collection
                        supply_tag_*    the "key field" is the Serial Number
                        demand_tag_*    the "key field" is the Open Seat ID
                        learning_tag_*  the "key field" is the Learning Activity ID
        :param is_debug:
        """
        BaseObject.__init__(self, __name__)
        self._is_debug = is_debug
        self._key_field = key_field
        self._collection_name = collection_name

    def _validate(self) -> None:
        from cendalytics.skills.core.dto import SkillsReportValidator

        if not self._collection_name or type(self._collection_name) != str:
            SkillsReportValidator.expected(self._collection_name, "str")

        if self._key_field and type(self._key_field) != str:
            SkillsReportValidator.expected(self._key_field, "str")

    def initialize(self) -> PerformKeyFieldSearch:
        """
        :return:
            an instantiated service
        """

        # Step: Perform Data Validation
        self._validate()

        self.logger.info(f"Instantiated Search ("
                         f"key-field={self._key_field})")

        # Step: Instantiate Service
        return PerformKeyFieldSearch(is_debug=self._is_debug,
                                     key_field=self._key_field,
                                     collection_name=self._collection_name)
