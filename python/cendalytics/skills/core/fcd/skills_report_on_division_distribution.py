# !/usr/bin/env python
# -*- coding: UTF-8 -*-


from base import BaseObject
from base import DataTypeError
from cendalytics.skills.core.svc import GenerateDivisionDistribution


class SkillsReportOnDivisionDistribution(BaseObject):
    """ Facade: return a record counter by distribution """

    def __init__(self,
                 collection_name: str,
                 is_debug: bool = False):
        """
        Created:
            8-Nov-2019
            craig.trim@ibm.com
            *   refactored out of skills-report-api
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1293
        :param collection_name:
            Any valid collection with a div_name attribute
        :param is_debug:
        """
        BaseObject.__init__(self, __name__)

        self._is_debug = is_debug
        self._collection_name = collection_name

    def _validate(self):
        if not self._collection_name or type(self._collection_name) != str:
            raise DataTypeError("Collection Name")

    def initialize(self) -> GenerateDivisionDistribution:
        """
        :return:
            an instantiated service
        """

        # Step: Perform Data Validation
        self._validate()

        self.logger.info('\n'.join([
            "Instantiate Division Distribution Report",
            f"\tCollection: {self._collection_name}"]))

        # Step: Instantiate Service
        return GenerateDivisionDistribution(collection_name=self._collection_name,
                                            is_debug=self._is_debug)
