# !/usr/bin/env python
# -*- coding: UTF-8 -*-


from typing import ValuesView

from base import BaseObject


class FindPatterns(BaseObject):
    """ One-Stop-Shop Service API for entity patterns (variations) """

    _d_patterns = None
    _long_distance = None

    def __init__(self,
                 ontology_name: str = 'base',
                 is_debug: bool = False):
        """
        Created:
            26-Mar-2019
            craig.trim@ibm.com
            *   based on 'find-entity'
        Updated:
            15-Jul-2019
            craig.trim@ibm.com
            *   add 'find' method
        Updated:
            13-Dec-2019
            craig.trim@ibm.com
            *   load dictionaries by ontology name
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1582
        """
        BaseObject.__init__(self, __name__)
        from datadict.core.dmo import DictionaryLoader

        _loader = DictionaryLoader(is_debug=is_debug,
                                   ontology_name=ontology_name)

        self._d_patterns = _loader.taxonomy().patterns()

    def find(self,
             input_text: str,
             include_patterns: bool = True) -> list or None:
        input_text = input_text.lower().strip()

        def _results(a_key: str):
            if include_patterns:
                return self._d_patterns[a_key]
            return [x for x in self._d_patterns[a_key]
                    if "+" not in x]

        for k in self._d_patterns:
            if k.lower() == input_text:
                return _results(k)

    def long_distance(self) -> ValuesView[list]:
        """
        sample input:
            {   'Aix 5.2 Workload': [   'aix+5.2+workload',
                                        'aix_5.2_workload' ],
                'Aix 5.2 Workload Partitions':  [   'aix+5.2+workload+partitions',
                                                    'aix_5.2_workload_partitions' ],
                ...
            }

        sample output:
            [   {   "pattern":  [aix, 5.2],
                    "label":    'Aix 5.2 Workload' },
                {   "pattern":  [aix, 5.2, workload, partitions],
                    "label":    'Aix 5.2 Workload Partitions' }
                ...
            }

        this dictionary is typically used in long-distance matching algorithms in NLU

        :return:
             a dictionary with tuples as keys
             and values as the actual label
        """
        if self._long_distance is None:
            d_long_distance = {}

            for k in self._d_patterns:
                patterns = [x for x in self._d_patterns[k] if "+" in x]
                patterns = [[y.strip().lower() for y in x.split("+") if y]
                            for x in patterns if x]

                for pattern in patterns:

                    key = "".join(sorted(set(pattern)))
                    if key not in d_long_distance:
                        d_long_distance[key] = []

                    d_long_distance[key].append({
                        "pattern": pattern,
                        "label": k
                    })
            self._long_distance = d_long_distance.values()

        return self._long_distance
