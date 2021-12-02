# !/usr/bin/env python
# -*- coding: UTF-8 -*-


import time
from typing import Union

from pandas import DataFrame

from base import BaseObject
from datadict import FindCertifications
from datamongo import BaseMongoClient
from datamongo import CendantTag


class FindSelfReportedCertifications(BaseObject):
    """
    Purpose:
    Service that finds Self-Reported Certifications in user CVs and HR data

    a "self-reported" certification is one that cannot be externally verifiable.

    Examples:
        1.  an IBM employee may earn a Level 2 badge Certification from IBM for Architecture.
            This can be verified using data we have.
        2.  an IBM employee can earn a certification that results in a badge.
            This can be verified - IF - IBM has access to that particular badge data
            (IBM can access some, but not all, badges)
        3.  an IBM employee can earn a Microsoft certification
            While this can be verified through a Microsoft directory, we likely will not have access to that data
            and therefore can provide an automated system confirmation.  Thus, this would remain an example of a
            "self-reported certification" meaning there is no automated independent verification possible.

    Traceability:
    https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/606

    Prereq:
    a populated "Supply_Tag_***" collection (e.g., 'supply_tag_20190801')
    """

    def __init__(self,
                 collection_name: str,
                 exclude_vendors: list,
                 add_normalized_text: bool,
                 aggregate_data: bool,
                 mongo_database_name: str,
                 server_alias: str = 'cloud',
                 is_debug: bool = False):
        """
        Created:
            5-Aug-2019
            craig.trim@ibm.com
       Purpose:
            Execute the Report to find "Self-Reported Certifications"
        :param collection_name:
            the collection from which to query data
            this should be the most recent annotation run against 'supply-tag' (e.g., supply_tag_20190801)
        :param add_normalized_text:
            add the normalized (pre-processed) field text to the dataframe result
            Reference:
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/815#issuecomment-14125072
        :param aggregate_data:
            if True         return aggregate data only
                            [Certification, Vendor, Frequency]
        :param exclude_vendors:
            Optional        a list of vendors to exclude
                            this is not case sensitive
                            for example, ['ibm'] will exclude all IBM certifications from this feedback
        :param mongo_database_name:
            the database containing the MongoDB collections (e.g., cendant)
        :param mongo_host_name:
            deprecated/ignored
        :return:
            a pandas DataFrame with the results
        :param is_debug:
            if True     increase log output at DEBUG level
        """
        BaseObject.__init__(self, __name__)

        self._is_debug = is_debug
        self._aggregate_data = aggregate_data
        self._exclude_vendors = exclude_vendors
        self._add_normalized_text = add_normalized_text

        self._cert_finder = FindCertifications()
        self._collection = CendantTag(collection_name=collection_name,
                                      database_name=mongo_database_name,
                                      mongo_client=BaseMongoClient(server_alias=server_alias),
                                      is_debug=True)

        if self._is_debug:
            self.logger.debug('\n'.join([
                f"\tInitialize FindSelfReportedCertifications: ",
                f"\tDatabase Name: {mongo_database_name}",
                f"\tCollection Name: {collection_name}",
                f"\tAggregate Data: {self._aggregate_data}",
                f"\tExclude Vendors: {self._exclude_vendors}",
                f"\tAdd Normalized Text: {self._add_normalized_text}"]))

    def _result(self) -> Union[DataFrame, None]:
        """
        Purpose:
        1.  Individual Dataframe (default)
            https://github.ibm.com/-cdo/unstructured-analytics/issues/617

            This dataframe returns a feedback record-by-record
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

            Each Serial Number, Certification and Vendor is represented

        2.  Aggregated dataframe
            https://github.ibm.com/-cdo/unstructured-analytics/issues/606

            This dataframe returns aggregated statistics at the Certificatiomn level
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

            Serial Numbers are left out of this.

        :return:
            the result Dataframe
        """
        from cendalytics.skills.core.dmo import CertificationAggregateReport
        from cendalytics.skills.core.dmo import CertificationIndividualReport

        if self._aggregate_data:
            if self._is_debug:
                self.logger.debug("Running Aggregate Report")
            return CertificationAggregateReport(collection=self._collection,
                                                is_debug=self._is_debug).process()

        if self._is_debug:
            self.logger.debug("Running Individual Report")
        return CertificationIndividualReport(collection=self._collection,
                                             is_debug=self._is_debug).process()

    def _exclusions(self,
                    dataframe: DataFrame) -> DataFrame:
        """
        Purpose:
            Exclude Blacklisted Vendors
            https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/624
        :param dataframe:
            the original dataframe
        :return:
            the modified dataframe
        """
        for vendor in self._exclude_vendors:
            dataframe = dataframe[dataframe.Vendor != vendor]

        return dataframe

    def _subsumptions(self,
                      dataframe: DataFrame) -> DataFrame:
        """
        Purpose:
            Remove Subsumed Certifications
            https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/623
        :param dataframe:
            the original dataframe
        :return:
            the modified dataframe
        """
        from cendalytics.skills.core.svc import RemoveSubsumedCertifications
        return RemoveSubsumedCertifications(dataframe=dataframe,
                                            is_debug=self._is_debug).process()

    def _label_transformation(self,
                              dataframe: DataFrame) -> DataFrame:
        """
        Purpose:
            Perform Certification Labelling
            https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/981
        :param dataframe:
            the original dataframe
        :return:
            the modified dataframe
        """
        from cendalytics.skills.core.svc import PerformCertificationLabelling
        return PerformCertificationLabelling(dataframe=dataframe,
                                             is_debug=self._is_debug).process()

    def process(self) -> DataFrame:
        start = time.time()

        df_results = self._result()
        if df_results is None or df_results.empty:
            raise ValueError(f"No Records Found "
                             f"(collection={self._collection.collection.collection_name})")

        df_results = self._exclusions(df_results)
        df_results = self._subsumptions(df_results)

        if self._is_debug:
            end_time = time.time() - start
            self.logger.debug(f"Generated Report: "
                              f"Time={end_time}, "
                              f"Total={len(df_results)}")

        return df_results.sort_values(by=['Confidence'],
                                      ascending=False)
