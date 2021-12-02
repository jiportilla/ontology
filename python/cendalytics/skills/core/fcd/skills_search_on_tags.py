# !/usr/bin/env python
# -*- coding: UTF-8 -*-


from base import BaseObject
from base import DataTypeError
from base import MandatoryParamError
from cendalytics.skills.core.svc import PerformTagSearch


class SkillsSearchOnTags(BaseObject):
    """ Facade: Service that Performs a Search against Tags (annotations) in MongoDB """

    def __init__(self,
                 tags: list,
                 collection_name: str,
                 div_field: str or list,
                 key_field: str,
                 server_alias: str = 'cloud',
                 is_debug: bool = False):
        """
        Created:
            8-Nov-2019
            craig.trim@ibm.com
            *   refactored out of skills-report-api
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1293
        Updated:
            12-Nov-2019
            craig.trim@ibm.com
            *   added 'normalize-tags-field' in pursuit of
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1334
        Updated:
            13-Feb-2020
            craig.trim@ibm.com
            *   Pass in Server Alias as a parameter
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1855
        :param tags:
            the list of tags to search on
            the default search is OR
        :param collection_name:
            the name of the collection to perform the skills search in
        :param div_field:
            the division (e.g., 'GTS' or 'GBS') to restrict searches for
            if None     search all divisions
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
        self._server_alias = server_alias
        self._collection_name = collection_name

        self._tags = self._normalize_tags_field(tags)
        self._div_field = self._normalize_div_field(div_field)

    @staticmethod
    def _normalize_tags_field(tags) -> list:
        if type(tags) == list:
            return tags
        if type(tags) == str:
            return [tags]

    @staticmethod
    def _normalize_div_field(div_field: str or list) -> list:
        if not div_field or not len(div_field):  # 1201#issuecomment-15702053
            return []
        if type(div_field) == str:  # 1201#issuecomment-15702050
            return [div_field]
        return div_field

    def _validate(self) -> None:
        if not self._tags or not len(self._tags):
            raise MandatoryParamError("Tags")
        if type(self._tags) != list:
            raise DataTypeError

    def initialize(self) -> PerformTagSearch:
        """
        :return:
            the dataframe of query results
        """

        # Step: Perform Data Validation
        # self._validate()

        self.logger.info('\n'.join([
            "Instantiate Tag Search",
            f"\tTags: {self._tags}",
            f"\tDivision: {self._div_field}",
            f"\tKey Field: {self._key_field}",
            f"\tCollection: {self._collection_name}"]))

        # Step: Instantiate Service
        return PerformTagSearch(tags=self._tags,
                                div_field=self._div_field,
                                key_field=self._key_field,
                                server_alias=self._server_alias,
                                collection_name=self._collection_name,
                                is_debug=self._is_debug)
