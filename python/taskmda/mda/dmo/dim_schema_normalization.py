# !/usr/bin/env python
# -*- coding: UTF-8 -*-



from typing import Optional
import pandas as pd
from pandas import DataFrame
from tabulate import tabulate

from base import BaseObject


class DimSchemaNormalization(BaseObject):
    """
    Purpose:
        Flatten a given Dimensionality schema with arbitrary nesting
    Sample Input:
        {   'hard skill':
            {   'big data':
                [   {   'big data l2':
                        [   'here',
                            'we',
                            'are'
                        ]},
                    'big data',
                    'spark',
                    'hadoop'
                ]
            }
        }
    Sample Output:
        +----+-------------+-------------+--------+
        |    | Child       | Parent      | Type   |
        |----+-------------+-------------+--------|
        |  0 | here        | big data l2 | L1-2   |
        |  1 | we          | big data l2 | L1-2   |
        |  2 | are         | big data l2 | L1-2   |
        |  3 | big data l2 | big data    | L2-2   |
        |  4 | big data    | big data    | L1-1   |
        |  5 | spark       | big data    | L1-1   |
        |  6 | hadoop      | big data    | L1-1   |
        |  7 | big data    | hard skill  | L2-1   |
        +----+-------------+-------------+--------+
    Notes:
        the 'type' column is a debug-level construct
    """

    def __init__(self,
                 d_schema: dict,
                 is_debug: bool = False):
        """
        Created:
            29-Oct-2019
            craig.trim@ibm.com
            *   re-write of original entity-schema-finder
        """
        BaseObject.__init__(self, __name__)

        self._is_debug = is_debug
        self._d_schema = d_schema

    def _transform(self) -> DataFrame:
        results = []

        def _add(a_parent: str,
                 a_child: str,
                 add_type: str) -> None:
            if type(a_parent) == str and type(a_child) == str:
                results.append({
                    "Parent": a_parent,
                    "Child": a_child,
                    "Type": add_type})

        def _iter(a_dict: dict,
                  a_parent: Optional[str],
                  ctr: int) -> None:
            for k in a_dict:
                _add(k, k, f"L3-{ctr}")
                ktype = type(a_dict[k])
                if not a_dict[k] or ktype is str:
                    pass
                elif ktype == dict:
                    _iter(a_dict[k], k, ctr + 1)
                elif ktype == list:
                    for inner_value in list(a_dict[k]):
                        if type(inner_value) == dict:
                            _iter(inner_value, k, ctr + 1)
                        elif type(inner_value) == str:
                            _add(k, inner_value, f"L1-{ctr}")
                else:
                    raise NotImplementedError(ktype)
                if a_parent:
                    _add(a_parent, k, f"L2-{ctr}")

        _iter(self._d_schema, None, 0)

        return pd.DataFrame(results)

    def process(self) -> DataFrame:
        df = self._transform()

        if self._is_debug:
            self.logger.debug('\n'.join([
                "Dimesionality DataFrame Generated",
                tabulate(df, headers="keys", tablefmt="psql")]))

        return df
