#!/usr/bin/env python
# -*- coding: UTF-8 -*-


import pandas as pd
from pandas import DataFrame

from base import BaseObject
from datamongo import BaseMongoClient


class GenerateZcores(BaseObject):
    """
    Purpose:
    """

    def __init__(self,
                 df_cnums: DataFrame,
                 df_means: DataFrame,
                 mongo_client: BaseMongoClient,
                 is_debug: bool = False):
        """
        Created:
            1-Oct-2019
            craig.trim@ibm.com
            *   transposed DataFrame
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1029
        Updated:
            3-Oct-2019
            craig.trim@ibm.com
            *   added jrs information
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1039
        :param df_cnums:
            +------+-------------+----------------+---------------------+----------+
            |      |   JobRoleId | SerialNumber   | Slot                |   Weight |
            |------+-------------+----------------+---------------------+----------|
            |    0 |      042393 | 050484766      | Blockchain          |   0      |
            |    1 |      042393 | 050484766      | Quantum             |   0      |
            |    2 |      042393 | 050484766      | Cloud               |   0      |
            |    3 |      042393 | 050484766      | SystemAdministrator |   4.022  |
            |    4 |      042393 | 050484766      | Database            |   0      |
            ...
            | 1098 |      042393 | 9D5863897      | ProjectManagement   |   0      |
            | 1099 |      042393 | 9D5863897      | ServiceManagement   |   4.411  |
            +------+-------------+----------------+---------------------+----------+
        :param df_means:
            +----+---------------------+---------+----------+
            |    | Slot                |   Stdev |   Mean   |
            |----+---------------------+---------+----------|
            |  0 | Blockchain          |   0.977 |    0.226 |
            |  1 | Cloud               |   4.46  |    2.683 |
            |  2 | DataScience         |  10.156 |    5.492 |
            |  3 | Database            |   6.858 |    3.404 |
            |  4 | HardSkill           |  16.744 |   19.523 |
            |  5 | Other               |  10.075 |    8.86  |
            |  6 | ProjectManagement   |  12.84  |   11.175 |
            |  7 | Quantum             |   0.107 |    0.032 |
            |  8 | ServiceManagement   |  13.014 |   11.658 |
            |  9 | SoftSkill           |  16.234 |   18.07  |
            | 10 | SystemAdministrator |  12.323 |    8.431 |
            +----+---------------------+---------+----------+
        :param augment_with_jrs_data:
        :param is_debug:
        """
        BaseObject.__init__(self, __name__)
        self.is_debug = is_debug
        self._df_cnums = df_cnums
        self._df_means = df_means
        self._mongo_client = mongo_client

    @staticmethod
    def _zscore(cnum_slot_weight: float,
                slot_mean: float,
                slot_stdev: float) -> float:
        """
        Purpose:
            Compute a z-Score
        :param cnum_slot_weight:
            the Weight of a Slot for a given CNUM (Serial Number)
        :param slot_mean:
            the Mean Weight for a Slot across a population
        :param slot_stdev:
            the Std. Dev. of a Slot across a population
        :return:
            a z-score
        """

        def _stdev():
            if slot_stdev <= 0.0:
                return 0.001
            return slot_stdev

        z = round((cnum_slot_weight - slot_mean) / _stdev(), 0)
        if z == -0.0:  # yes, this happens
            return 0.0
        return z

    def _build_jrs_dataframes(self) -> dict:
        """
        Purpose:
            for each Job Role ID in the incoming dataframe
            create a JRS DataFrame association
        :return:
            a dictionary of JRS IDs to DF lookups:
                +----+---------------------------------------------------------------------------------------------------------------------------------------------------------------+
                |    | Description                                     |   JobRoleId | JobRoleName         |   NumberOfSkills | PrimaryJobCategory   | SecondaryJobCategory          |
                |----+---------------------------------------------------------------------------------------------------------------------------------------------------------------|
                |  0 | This role supervises, coordinates and maintains |      042393 | Service Coordinator |                9 | Technical Specialist | Technical Services Specialist |
                +----+---------------------------------------------------------------------------------------------------------------------------------------------------------------+
        """
        from cendantdim.jrs.svc import LookupJobRoleId

        d_jr_dfs = {}
        job_role_ids = self._df_cnums['JobRoleId'].unique()
        for job_role_id in job_role_ids:
            d_jr_dfs[job_role_id] = LookupJobRoleId(job_role_id=job_role_id,
                                                    mongo_client=self._mongo_client,
                                                    is_debug=self.is_debug).process()

        if self.is_debug:
            self.logger.debug(f"Created JRS DataFrame Lookup ("
                              f"total={len(d_jr_dfs)})")

        return d_jr_dfs

    def process(self):
        results = []

        d_jrs_lookup = self._build_jrs_dataframes()
        for cnum in self._df_cnums['SerialNumber'].unique():

            df_cnum = self._df_cnums[self._df_cnums['SerialNumber'] == cnum]
            job_role_id = df_cnum['JobRoleId'].unique()[0]

            for slot in df_cnum['Slot'].unique():
                df_mean_slot = self._df_means[self._df_means['Slot'] == slot]
                slot_mean = df_mean_slot['Mean'].unique()[0]
                slot_stdev = df_mean_slot['Stdev'].unique()[0]

                df_cnum_slot = df_cnum[df_cnum['Slot'] == slot]

                weight = df_cnum_slot['Weight'].unique()[0]

                zscore = self._zscore(cnum_slot_weight=weight,
                                      slot_mean=slot_mean,
                                      slot_stdev=slot_stdev)

                harmonic_mean = round((slot_mean + weight) / 2, 2)
                harmonic_mean_delta = harmonic_mean - weight

                def _jrs_value(field_name: str) -> list:
                    try:
                        return list(d_jrs_lookup[job_role_id][field_name].unique())
                    except KeyError:
                        return []

                jrs_description = _jrs_value('Description')[0]
                jrs_role_name = _jrs_value('JobRoleName')[0]

                results.append({
                    "SerialNumber": cnum,
                    "Slot": slot,
                    "Weight": weight,
                    "SlotMean": slot_mean,
                    "SlotStdev": slot_stdev,
                    "hMean": harmonic_mean,
                    "hDelta": harmonic_mean_delta,
                    "zScore": zscore,
                    "JobRoleId": job_role_id,
                    "JobRoleDescription": jrs_description,
                    "JobRoleName": jrs_role_name})

        return pd.DataFrame(results)
