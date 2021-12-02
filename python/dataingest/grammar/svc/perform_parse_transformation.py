#!/usr/bin/env python
# -*- coding: UTF-8 -*-


import os

from pandas import DataFrame

from base import BaseObject


class PerformPythonTransformation(BaseObject):
    """ Transform Python Dependency Analysis into Source Format

    Sample Input:
        +-----+-------------------------------------------------------------------------------------------------------------------------------------------+-----------+---------------+---------------+----------+------+-------------------------------------+--------------------------------+----------------+----------+-------+------+--------------------------------------------------------------------------------------+------------------+
        |     | AbsolutePath                                                                                                                              | D0        | D1            | D2            | D3       | D4   | FileName                            | Import                         | L0             | L1       | L2    | L3   | RelativePath                                                                         | SOAType          |
        |-----+-------------------------------------------------------------------------------------------------------------------------------------------+-----------+---------------+---------------+----------+------+-------------------------------------+--------------------------------+----------------+----------+-------+------+--------------------------------------------------------------------------------------+------------------|
        |   0 | /Users/craig.trimibm.com/data/workspaces/ibm/text/workspace/dataingest/dataingest/grammar/bp/python_parse_api.py                 | workspace | dataingest | dataingest | grammar  | bp   | python_parse_api.py                 | BaseObject                     | base        | nan      | nan   | nan  | workspace/dataingest/dataingest/grammar/bp/python_parse_api.py                 | Business Process |
        |   1 | /Users/craig.trimibm.com/data/workspaces/ibm/text/workspace/dataingest/dataingest/grammar/bp/python_parse_api.py                 | workspace | dataingest | dataingest | grammar  | bp   | python_parse_api.py                 | PerformPythonTransformation    | dataingest  | grammar  | svc   | nan  | workspace/dataingest/dataingest/grammar/bp/python_parse_api.py                 | Business Process |
        |   2 | /Users/craig.trimibm.com/data/workspaces/ibm/text/workspace/dataingest/dataingest/grammar/bp/python_parse_api.py                 | workspace | dataingest | dataingest | grammar  | bp   | python_parse_api.py                 | ParsePythonFiles            | dataingest  | grammar  | svc   | nan  | workspace/dataingest/dataingest/grammar/bp/python_parse_api.py                 | Business Process |
        |   3 | /Users/craig.trimibm.com/data/workspaces/ibm/text/workspace/dataingest/dataingest/grammar/svc/perform_python_parse.py            | workspace | dataingest | dataingest | grammar  | svc  | perform_python_parse.py             | Optional                       | typing         | nan      | nan   | nan  | workspace/dataingest/dataingest/grammar/svc/perform_python_parse.py            | Service          |
        |   4 | /Users/craig.trimibm.com/data/workspaces/ibm/text/workspace/dataingest/dataingest/grammar/svc/perform_python_parse.py            | workspace | dataingest | dataingest | grammar  | svc  | perform_python_parse.py             | DataFrame                      | pandas         | nan      | nan   | nan  | workspace/dataingest/dataingest/grammar/svc/perform_python_parse.py            | Service          |
        |   5 | /Users/craig.trimibm.com/data/workspaces/ibm/text/workspace/dataingest/dataingest/grammar/svc/perform_python_parse.py            | workspace | dataingest | dataingest | grammar  | svc  | perform_python_parse.py             | tabulate                       | tabulate       | nan      | nan   | nan  | workspace/dataingest/dataingest/grammar/svc/perform_python_parse.py            | Service          |
        ...
        | 285 | /Users/craig.trimibm.com/data/workspaces/ibm/text/workspace/dataingest/dataingest/github/dmo/comments_transformation.py          | workspace | dataingest | dataingest | github   | dmo  | comments_transformation.py          | MandatoryParamError            | base        | nan      | nan   | nan  | workspace/dataingest/dataingest/github/dmo/comments_transformation.py          | Domain Component |
        | 286 | /Users/craig.trimibm.com/data/workspaces/ibm/text/workspace/dataingest/dataingest/github/dmo/comments_transformation.py          | workspace | dataingest | dataingest | github   | dmo  | comments_transformation.py          | BaseObject                     | base        | nan      | nan   | nan  | workspace/dataingest/dataingest/github/dmo/comments_transformation.py          | Domain Component |
        +-----+-------------------------------------------------------------------------------------------------------------------------------------------+-----------+---------------+---------------+----------+------+-------------------------------------+--------------------------------+----------------+----------+-------+------+--------------------------------------------------------------------------------------+------------------+
    """

    def __init__(self,
                 df_parse: DataFrame,
                 collection_name: str,
                 is_debug: bool = False):
        """
        Created:
            6-Dec-2019
            craig.trim@ibm.com
            *   https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1535
        :param is_debug:
        """
        BaseObject.__init__(self, __name__)

        self._is_debug = is_debug
        self._df_parse = df_parse
        self._collection_name = collection_name

    def _path(self):
        return os.path.join(os.environ['CODE_BASE'],
                            'resources/output/transform',
                            f"{self._collection_name}.csv")

    def process(self):
        outpath = self._path()
        self.logger.debug('\n'.join([
            f"Wrote to File (size={len(self._df_parse)})",
            f"\tPath: {self._path()}"]))

        self._df_parse.to_csv(outpath, encoding='utf-8', sep='\t')
