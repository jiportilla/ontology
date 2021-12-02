# !/usr/bin/env python
# -*- coding: UTF-8 -*-


from base import BaseObject
from cendalytics.skills.core.svc import PerformNormalizedTextSearch


class SkillsSearchOnNormalizedText(BaseObject):
    """ Facade: Service that Performs a Full-Text Search on normalized text in MongoDB """

    def __init__(self,
                 collection_name: str,
                 search_terms: list = None,
                 div_field: str or list = None,
                 key_field: str = None,
                 is_debug: bool = False):
        """
        Created:
            8-Nov-2019
            craig.trim@ibm.com
            *   refactored out of skills-report-api
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1293
        :param search_terms:
            a list of one or more terms to search MongoDB for
            None        the key-field must have a value in this case
        :param collection_name:
            the name of the collection to perform the skills search in
        :param div_field:
            the list of divisions (e.g., 'GTS' or 'GBS') to restrict searches for
            if None     search all divisions
            Note:
                Use the API method 'division-distribution' to find existing divisions
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
        if not div_field:
            div_field = []

        self._is_debug = is_debug
        self._key_field = key_field
        self._search_terms = search_terms
        self._collection_name = collection_name
        self._div_field = self._normalize_div_field(div_field)

    def _validate(self) -> None:
        from cendalytics.skills.core.dto import SkillsReportValidator

        if not self._collection_name or type(self._collection_name) != str:
            SkillsReportValidator.expected(self._collection_name, "str")

    @staticmethod
    def _normalize_div_field(div_field: str or list) -> list:
        if not div_field or not len(div_field):  # 1201#issuecomment-15702053
            return []
        if type(div_field) == str:  # 1201#issuecomment-15702050
            return [div_field]
        return div_field

    def initialize(self) -> PerformNormalizedTextSearch:
        """
        :return:
            an instantiated service
        """

        # Step: Perform Data Validation
        self._validate()

        self.logger.info('\n'.join([
            "Instantiate Normalized Text Search",
            f"\tSearch Terms: {self._search_terms}",
            f"\tDivision: {self._div_field}",
            f"\tKey Field: {self._key_field}"]))

        # Step: Instantiate Service
        return PerformNormalizedTextSearch(is_debug=self._is_debug,
                                           key_field=self._key_field,
                                           div_field=self._div_field,
                                           search_terms=self._search_terms,
                                           collection_name=self._collection_name)
