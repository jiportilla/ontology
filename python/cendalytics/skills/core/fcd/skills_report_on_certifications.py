# !/usr/bin/env python
# -*- coding: UTF-8 -*-

from typing import Optional

from base import BaseObject
from base import DataTypeError
from base import MandatoryParamError
from cendalytics.skills.core.svc import FindSelfReportedCertifications


class SkillsReportOnCertifications(BaseObject):
    """ Facade: Service that finds Self-Reported Certifications in user CVs and HR data """

    def __init__(self,
                 collection_name: str,
                 exclude_vendors: Optional[list],
                 add_normalized_text: bool = True,
                 aggregate_data: bool = False,
                 mongo_database_name: str = 'cendant',
                 is_debug: bool = False):
        """
        Created:
            8-Nov-2019
            craig.trim@ibm.com
            *   refactored out of skills-report-api
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1293
        :param collection_name:
            the collection from which to query data
            this should be the most recent annotation run against 'supply-tag' (e.g., supply_tag_20190801)
        :param exclude_vendors:
            Optional        a list of vendors to exclude
                            this is not case sensitive
                            for example, ['ibm'] will exclude all IBM certifications from this feedback
        :param add_normalized_text:
            add the normalized (pre-processed) field text to the dataframe result
            Reference:
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/815#issuecomment-14125072
        :param aggregate_data:
            if True         return aggregate data only
            +----+-------------------------------------------------+-------------+----------------+
            |    | Certification                                   |   Frequency | Vendor         |
            |----+-------------------------------------------------+-------------+----------------|
            |  0 | ITIL Certification                              |        8176 | Axelos         |
            |  1 | ITIL Foundation Certification                   |       15450 | Axelos         |
            |  2 | IBM Certification                               |       21062 | IBM            |
            |  3 | CCNA Certification                              |        4899 | Cisco          |
            |  4 | Java Certification                              |        2224 | Oracle         |
            |  5 | Project Management Professional                 |        1660 | PMI            |
            |  6 | Level 3 Certification                           |         415 | IBM            |
            |  7 | CCNA Security                                   |         142 | Cisco          |
            |  8 | Microsoft Certification                         |        1880 | Microsoft      |
            +----+-------------------------------------------------+-------------+----------------+
            if False        return individual level data
            +----+-------------------------------------------------+----------------+----------------+
            |    | Certification                                   | SerialNumber   | Vendor         |
            |----+-------------------------------------------------+----------------+----------------|
            |  0 | ITIL Certification                              | 123456         | Axelos         |
            |  1 | ITIL Foundation Certification                   | 227232         | Axelos         |
            |  2 | IBM Certification                               | 9483223        | IBM            |
            |  3 | CCNA Certification                              | 9483223        | Cisco          |
            |  4 | Java Certification                              | 9483223        | Oracle         |
            |  5 | Project Management Professional                 | 923823         | Oracle         |
            |  6 | Level 3 Certification                           | 009238323      | IBM            |
            |  7 | CCNA Security                                   | 009238323      | Cisco          |
            |  8 | Microsoft Certification                         | 2371221        | Microsoft      |
            +----+-------------------------------------------------+----------------+----------------+
        :param mongo_database_name:
            the database containing the MongoDB collections (e.g., cendant)
        :param is_debug:
        """
        BaseObject.__init__(self, __name__)

        self._is_debug = is_debug
        self._aggregate_data = aggregate_data
        self._collection_name = collection_name
        self._mongo_database_name = mongo_database_name
        self._add_normalized_text = add_normalized_text
        self._exclude_vendors = self._transform_exclude_vendors(exclude_vendors)

    @staticmethod
    def _transform_exclude_vendors(exclude_vendors: Optional[list]) -> list:
        if not exclude_vendors:
            return []
        return exclude_vendors

    def _validate(self) -> None:
        if not self._collection_name:
            raise MandatoryParamError("Collection Name")
        if type(self._collection_name) != str:
            raise DataTypeError

    def initialize(self) -> FindSelfReportedCertifications:
        """
        Purpose:
            Execute the Report to find "Self-Reported Certifications"
        :return:
            a pandas DataFrame with the results
        """

        # Step: Perform Data Validation
        self._validate()

        self.logger.info('\n'.join([
            "Instantiate Self-Reported Certifications Report",
            f"\tAggregate Data: {self._aggregate_data}",
            f"\tExclude Vendors: {self._exclude_vendors}",
            f"\tAdd Normalized Text: {self._add_normalized_text}",
            f"\tCollection: {self._collection_name}"]))

        # Step: Instantiate Service
        return FindSelfReportedCertifications(exclude_vendors=self._exclude_vendors,
                                              aggregate_data=self._aggregate_data,
                                              add_normalized_text=self._add_normalized_text,
                                              collection_name=self._collection_name,
                                              mongo_database_name=self._mongo_database_name,
                                              is_debug=self._is_debug)
