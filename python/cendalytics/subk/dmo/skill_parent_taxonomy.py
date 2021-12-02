# !/usr/bin/env python
# -*- coding: UTF-8 -*-


from typing import Optional

import pandas as pd
from pandas import DataFrame
from pandas import Series
from tabulate import tabulate

from base import BaseObject
from datadict import FindRelationships


class SkillParentTaxonomy(BaseObject):
    """ Cluster Input Skills by Parents (1, 2, 3) levels deep

    Sample Output:
        +-----+--------+-----------------------------+----------------------+----------------------+--------------------------+--------------------+
        |     |   Freq | Parent1                     | Parent2              | Parent3              | Skill                    | Tag                |
        |-----+--------+-----------------------------+----------------------+----------------------+--------------------------+--------------------|
        | 149 |      2 | shell script                | scripting language   | programming language | bash                     | Bash               |
        | 186 |      1 | web language                | programming language | formal language      | ember.js                 | Javascript         |
        | 241 |      1 | project management software | software             | product              | jira                     | JIRA               |
        | 344 |      1 | python library              | software library     | software             | scikit                   | Sklearn            |
        | 224 |      1 | mainframe operating system  | mainframe            | system               | aix                      | IBM AIX            |
        +-----+--------+-----------------------------+----------------------+----------------------+--------------------------+--------------------+
    """

    def __init__(self,
                 df_input: DataFrame,
                 is_debug: bool = True):
        """
        Created:
            16-Jan-2020
            craig.trim@ibm.com
            *   https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1740#issuecomment-17198866
        :param is_debug:
            if True     increase log output at DEBUG level
        """
        BaseObject.__init__(self, __name__)
        self._is_debug = is_debug
        self._df_input = df_input
        self._rel_finder = FindRelationships(is_debug=is_debug)

    def _taxonomy(self,
                  tags: list) -> dict:
        __blacklist = ['unknown']

        d_parents = {}

        def _parents(a_term: str) -> list:
            results = self._rel_finder.parents(a_term)
            results = [x.lower().strip() for x in results]
            results = [x for x in results if x not in __blacklist]

            return results

        for tag in tags:
            parents = _parents(tag)
            d_parents[tag] = parents

            for parent in parents:
                parents2 = _parents(parent)
                d_parents[parent] = parents2

                for parent2 in parents2:
                    d_parents[parent2] = _parents(parent2)

        return d_parents

    def _to_dataframe(self,
                      d_parents: dict,
                      generator) -> DataFrame:
        master = []

        def _get_parents(a_term: str) -> Optional[list]:
            if a_term.lower().strip() in d_parents:
                return d_parents[a_term.lower().strip()]

        for _, row in self._df_input.iterrows():

            for parent1 in _get_parents(row['Tag']):

                parents2 = _get_parents(parent1)
                if not parents2:
                    master.append(generator(row, parent1, None, None))

                for parent2 in parents2:
                    parents3 = _get_parents(parent2)

                    if not parents3:
                        master.append(generator(row, parent1, parent2, None))

                    for parent3 in parents3:
                        master.append(generator(row, parent1, parent2, parent3))

        return pd.DataFrame(master)

    def process(self,
                generator):


        tags = sorted(self._df_input['Tag'].unique())
        tags = [tag.lower().strip() for tag in tags]

        d_parents = self._taxonomy(tags)
        df_cluster = self._to_dataframe(d_parents, generator)

        if self._is_debug:
            self.logger.debug('\n'.join([
                "Input Skill Cluster Created",
                tabulate(df_cluster.sample(10), headers='keys', tablefmt='psql')]))

        return df_cluster
