#!/usr/bin/env python
# -*- coding: UTF-8 -*-


import pandas as pd
from pandas import DataFrame
from tabulate import tabulate

from base import BaseObject


class XdmRecordTransformation(BaseObject):
    """
    Purpose:
        Transform nested XDM record into a flat DataFrame

    Sample Input:
        {   'key_field': '0D5985649',
            'ts': '662e1e44-faaa-11e9-ad33-06c95b52e0d3'
            'slots': {
                'blockchain':               {'weight': 0.0, 'z_score': -0.4, 'z_score_norm': 0.0},
                'cloud':                    {'weight': 0.0, 'z_score': -0.4, 'z_score_norm': 0.0},
                'data science':             {'weight': 0.0975, 'z_score': 1.21, 'z_score_norm': 1.6},
                'database':                 {'weight': 0.0, 'z_score': -0.4, 'z_score_norm': 0.0},
                'experience':               {'weight': 0.0, 'z_score': 0.0, 'z_score_norm': 0.0},
                'hard skill':               {'weight': 0.0, 'z_score': -0.4, 'z_score_norm': 0.0},
                'learning':                 {'weight': 0.0, 'z_score': -0.4, 'z_score_norm': 0.0},
                'other':                    {'weight': 0.0, 'z_score': -0.4, 'z_score_norm': 0.0},
                'project management':       {'weight': 0.1943, 'z_score': 2.81, 'z_score_norm': 3.2},
                'quantum':                  {'weight': 0.0, 'z_score': -0.4, 'z_score_norm': 0.0},
                'service management':       {'weight': 0.0, 'z_score': -0.4, 'z_score_norm': 0.0},
                'soft skill':               {'weight': 0.0, 'z_score': -0.4, 'z_score_norm': 0.0},
                'system administrator':     {'weight': 0.0, 'z_score': -0.4, 'z_score_norm': 0.0}}}

    Sample Output:
        +----+---------------------+------------+-------------------+--------------------------------------+----------------------+--------------+--------------+
        |    | Collection          | KeyField   | PriorCollection   | RecordId                             | Slot                 |   SlotWeight |   SlotZScore |
        |----+---------------------+------------+-------------------+--------------------------------------+----------------------+--------------+--------------|
        |  0 | supply_xdm_20191029 | 0D5985649  |                   | 662e1e44-faaa-11e9-ad33-06c95b52e0d3 | blockchain           |       0      |        -0.4  |
        |  1 | supply_xdm_20191029 | 0D5985649  |                   | 662e1e44-faaa-11e9-ad33-06c95b52e0d3 | cloud                |       0      |        -0.4  |
        |  2 | supply_xdm_20191029 | 0D5985649  |                   | 662e1e44-faaa-11e9-ad33-06c95b52e0d3 | data science         |       0.0975 |         1.21 |
        |  3 | supply_xdm_20191029 | 0D5985649  |                   | 662e1e44-faaa-11e9-ad33-06c95b52e0d3 | database             |       0      |        -0.4  |
        |  4 | supply_xdm_20191029 | 0D5985649  |                   | 662e1e44-faaa-11e9-ad33-06c95b52e0d3 | hard skill           |       0      |        -0.4  |
        |  5 | supply_xdm_20191029 | 0D5985649  |                   | 662e1e44-faaa-11e9-ad33-06c95b52e0d3 | learning             |       0      |        -0.4  |
        |  6 | supply_xdm_20191029 | 0D5985649  |                   | 662e1e44-faaa-11e9-ad33-06c95b52e0d3 | other                |       0      |        -0.4  |
        |  7 | supply_xdm_20191029 | 0D5985649  |                   | 662e1e44-faaa-11e9-ad33-06c95b52e0d3 | project management   |       0.1943 |         2.81 |
        |  8 | supply_xdm_20191029 | 0D5985649  |                   | 662e1e44-faaa-11e9-ad33-06c95b52e0d3 | quantum              |       0      |        -0.4  |
        |  9 | supply_xdm_20191029 | 0D5985649  |                   | 662e1e44-faaa-11e9-ad33-06c95b52e0d3 | service management   |       0      |        -0.4  |
        | 10 | supply_xdm_20191029 | 0D5985649  |                   | 662e1e44-faaa-11e9-ad33-06c95b52e0d3 | soft skill           |       0      |        -0.4  |
        | 11 | supply_xdm_20191029 | 0D5985649  |                   | 662e1e44-faaa-11e9-ad33-06c95b52e0d3 | system administrator |       0      |        -0.4  |
        | 12 | supply_xdm_20191029 | 0D5985649  |                   | 662e1e44-faaa-11e9-ad33-06c95b52e0d3 | experience           |       0      |         0    |
        +----+---------------------+------------+-------------------+--------------------------------------+----------------------+--------------+--------------+
    """

    def __init__(self,
                 records: list,
                 collection_name: str,
                 is_debug: bool = False):
        """
        Created:
            1-Nov-2019
            craig.trim@ibm.com
            *   https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1238#issuecomment-15696868
        Updated:
            19-Feb-2020
            abhbasu3@in.ibm.com
            *   https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1862
            *   updated record_id
        :param records:
        :param is_debug:
        """
        BaseObject.__init__(self, __name__)

        self._records = records
        self._is_debug = is_debug
        self._collection_name = collection_name

    def process(self,
                unused: float = None) -> DataFrame:
        master = []

        for record in self._records:

            record_id = record["_id"]
            key_field = record["key_field"]

            for slot_name in record["slots"]:
                d_slot = record["slots"][slot_name]
                master.append({
                    "KeyField": key_field,
                    "Slot": slot_name,
                    "SlotWeight": d_slot["weight"],
                    "SlotZScore": d_slot["z_score"],
                    "Collection": self._collection_name})

        df = pd.DataFrame(master)

        if self._is_debug and not df.empty:
            self.logger.debug("\n".join([
                f"XDM Transformation Complete (collection={self._collection_name}, total={len(df)})",
                tabulate(df.sample(),
                         tablefmt='psql',
                         headers='keys')]))

        return df
