# !/usr/bin/env python
# -*- coding: UTF-8 -*-


import pandas as pd
from pandas import DataFrame

from base import BaseObject
from datadict import FindCertifications


class RemoveSubsumedCertifications(BaseObject):
    """
    Purpose:
    Remove Subsumed Certifications

    a "subsumed" certification refers to a situation where an individual may be tagged as
        "ITIL Certification"
        "ITIL Foundation Certification"

    in this case, a parent-child relationship exists:
        "ITIL Foundation Certification" is more specific (is a child) of "ITIL Certification"

    as a result, "ITIL Certification" is subsumed by "ITIL Foundation Certification"
        and the least specific certification in this relationship should be removed

    Traceability:
    https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/623
    """

    def __init__(self,
                 dataframe: DataFrame,
                 is_debug: bool = False):
        """
        Created:
            6-Aug-2019
            craig.trim@ibm.com
       Purpose:
            Execute the Report to find "Self-Reported Certifications"
        :param dataframe:
            dataframe
        :return:
            a pandas DataFrame with the results
        :param is_debug:
            if True     increase log output at DEBUG level
        """
        BaseObject.__init__(self, __name__)

        self._is_debug = is_debug
        self._dataframe = dataframe
        self._cert_finder = FindCertifications()

        if self._is_debug:
            self.logger.debug(f"Initialize RemoveSubsumedCertifications")

    def process(self) -> DataFrame:
        results = []

        # Step: List all the Serial Numbers
        serial_numbers = self._dataframe['SerialNumber'].unique()

        for serial_number in serial_numbers:

            # Step: Find the sub-Frame for each Serial Number
            df2 = self._dataframe[self._dataframe['SerialNumber'] == serial_number]

            invalid = set()
            certifications = df2['Certification'].unique()

            # Step: Find Invalid (Subsumed) Certifications
            if len(certifications) > 1:
                for c1 in certifications:
                    for c2 in certifications:
                        if c1 == c2:
                            continue
                        if self._cert_finder.is_ancestor(c1, c2):
                            invalid.add(c1)
                        elif self._cert_finder.is_ancestor(c2, c1):
                            invalid.add(c2)

            # Step: Add Results Back and skip Invalid Certs
            for _, row in df2.iterrows():
                if row["Certification"] not in invalid:
                    results.append({"Vendor": row["Vendor"],
                                    "FieldId": row["FieldId"],
                                    "Confidence": row["Confidence"],
                                    "Certification": row["Certification"],
                                    "Normalized": row["Normalized"],
                                    "Original": row["Original"],
                                    "SerialNumber": row["SerialNumber"]})

        return pd.DataFrame(results)
