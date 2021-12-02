# !/usr/bin/env python
# -*- coding: UTF-8 -*-


from typing import Union

import pandas as pd
from pandas import DataFrame

from base import BaseObject
from datadict import FindCertifications
from datamongo import CendantTag


class CertificationAggregateReport(BaseObject):
    """
    Purpose:
    Service that finds aggregated data on Self-Reported Certifications in user CVs and HR data

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

    Traceability:
    https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/617#issuecomment-13692586

    Prereq:
    a populated "Supply_Tag_***" collection (e.g., 'supply_tag_20190801')
    """

    def __init__(self,
                 collection: CendantTag,
                 is_debug: bool = False):
        """
        Created:
            5-Aug-2019
            craig.trim@ibm.com
       Purpose:
            Execute the Report to find "Self-Reported Certifications"
        :param collection:
            the instantiated mongoDB collection from which to query data
        :return:
            a pandas DataFrame with the results
        :param is_debug:
            if True     increase log output at DEBUG level
        """
        BaseObject.__init__(self, __name__)

        self._is_debug = is_debug
        self._collection = collection
        self._cert_finder = FindCertifications()

    def _to_dataframe(self,
                      results: list) -> DataFrame:
        dataframe_results = []

        for result in results:
            vendor = self._cert_finder.vendor_by_certification(result["Certification"])
            dataframe_results.append({"Certification": result["Certification"],
                                      "FieldId": result["FieldId"],
                                      "Vendor": vendor})

        return pd.DataFrame(results)

    def _cert_frequency(self,
                        certifications: list) -> list:
        results = []

        for chunk in self._collection.collection.by_chunks():
            for record in chunk:
                fields = [field for field in record["fields"]
                          if field["type"] == "long-text"]

                for field in fields:
                    tags = [tag for tag in field["tags"]["supervised"]
                            if tag in certifications]

                    for tag in tags:
                        results.append({
                            "Certification": tag,
                            "FieldId": field["field_id"]})

        return results

    def process(self) -> Union[DataFrame, None]:

        if not self._collection.collection.count():
            self.logger.warning(f"No Records Found: "
                                f"{self._collection.collection.log()}")
            return None

        certifications = self._cert_finder.certifications()
        results = self._cert_frequency(certifications)
        return self._to_dataframe(results)
