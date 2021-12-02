# !/usr/bin/env python
# -*- coding: UTF-8 -*-


from pandas import DataFrame

from base import BaseObject
from base import LabelFormatter


class DataTransformerForConference(BaseObject):
    """ Transforms a conference DataFrame to a normalized JSON dictionary
    """

    def __init__(self,
                 some_df: DataFrame):
        """
        Created:
            5-Apr-2019
            craig.trim@ibm.com
        """
        BaseObject.__init__(self, __name__)
        self.df = some_df

    @staticmethod
    def _cleanse(a_str: str) -> str:
        return str(a_str).replace("'", "")

    def _result(self,
                a_row,
                column_name: str,
                unknown="unknown") -> str:
        some_result = self._cleanse(a_row[column_name])
        if not some_result or not len(some_result):
            return unknown
        return some_result

    @staticmethod
    def _name(a_row) -> str:
        first_name = a_row["first_name"]
        last_name = a_row["last_name"]
        return "{} {}".format(LabelFormatter.camel_case(first_name),
                              LabelFormatter.camel_case(last_name))

    def _country_code(self,
                      a_row) -> str:
        return self._cleanse(a_row["country_code"])

    def _department(self,
                    a_row) -> str:
        return self._cleanse(a_row["department"])

    def _division(self,
                  a_row) -> str:
        return self._cleanse(a_row["division"])

    def _organization(self,
                      a_row) -> str:
        return self._cleanse(a_row["organization"])

    def _group(self,
               a_row) -> str:
        return self._cleanse(a_row["group"])

    def _ebu_code(self,
                  a_row) -> str:
        return self._cleanse(a_row["ebu_code"])

    def _geography(self,
                   a_row) -> str:
        return self._result(a_row, "geography",
                            unknown="Other Geo")

    def _region(self,
                a_row) -> str:
        return self._result(a_row, "region",
                            unknown="Other Region")

    def _country_name(self,
                      a_row) -> str:
        return self._result(a_row, "country_name",
                            unknown="Other Country")

    def _job_role_id(self,
                     a_row) -> str:
        return self._result(a_row, "job_role_id",
                            unknown="0")

    def _bu_code(self,
                 a_row) -> str:
        return self._result(a_row, "bu_code",
                            unknown="Other BU")

    def _bu_name(self,
                 a_row) -> str:
        return self._result(a_row, "bu_name",
                            unknown="Other BU Name")

    def _lob_code(self,
                  a_row) -> str:
        return self._result(a_row, "lob_code",
                            unknown="Other LOB")

    def process(self):
        records = []
        for i, row in self.df.iterrows():
            records.append({
                "name": self._name(row),
                "country_code": self._country_code(row),
                "department": self._department(row),
                "division": self._division(row),
                "organization": self._organization(row),
                "group": self._group(row),
                "ebu_code": self._ebu_code(row),
                "geography": self._geography(row),
                "region": self._region(row),
                "country_name": self._country_name(row),
                "job_role_id": self._job_role_id(row),
                "bu_code": self._bu_code(row),
                "bu_name": self._bu_name(row),
                "lob_code": self._lob_code(row),
            })

        self.logger.debug("\n".join([
            "Transformed Dataframe to Records",
            "\ttlen: {}".format(len(records))
        ]))

        return records
