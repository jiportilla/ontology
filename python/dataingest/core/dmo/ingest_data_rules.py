#!/usr/bin/env python
# -*- coding: UTF-8 -*-


from pandas import DataFrame

from base import BaseObject


class IngestDataRules(BaseObject):
    """ Apply any data rules specified in the ingest manifest
        failure to meet these rules typically halts the ingest process with an error """

    def __init__(self):
        """
        Created:
            26-Apr-2019
            craig.trim@ibm.com
        """
        BaseObject.__init__(self, __name__)

    def process(self,
                d_manifest: dict,
                df_src: DataFrame) -> None:
        """
        Purpose:
            apply rules for missing data and halt the ingest if rules are broken
        :param d_manifest:
            the manifest
        :param df_src:
            the ingested dataframe prior to flows or transformation
        """
        if "missing_data" not in d_manifest["source"]:
            return

        for rule in d_manifest["source"]["missing_data"]:

            total_missing = 0
            total_missing += (df_src[rule["source_name"]].values == '').sum()
            total_missing += (df_src[rule["source_name"]].values == 'none').sum()
            total_missing += (df_src[rule["source_name"]].values == 'None').sum()
            total_missing += (df_src[rule["source_name"]].values == '0').sum()

            if total_missing > rule["tolerance"]:
                raise ValueError("\n".join([
                    "Missing Data Exceeds Tolerance Level",
                    "\tRule: {}".format(rule),
                    "\tTotal Missing: {}".format(total_missing)]))

            self.logger.debug("\n".join([
                "Missing Data Rule Requirements Met",
                "\tRule: {}".format(rule),
                "\tTotal Missing: {}".format(total_missing)]))
