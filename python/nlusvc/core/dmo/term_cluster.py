#!/usr/bin/env python
# -*- coding: UTF-8 -*-


from random import randint

from base import BaseObject
from base import MandatoryParamError


class TermCluster(BaseObject):
    """
        find the terms that are unique to each key
        these terms are the most discriminating for that key

        sample input:
            {   cisco:      {   router:     2662,
                                configure:  2453,
                                avaya:      2167    },
                microsoft:  {   configure:  11229,
                                citrix:     9650,
                                vsphere:    2942    }
                ...
                oracle:     {   configure:  2422,
                                hacmp:      822,
                                hana:       811    } }

        sample output:
            {   cisco:      [   router, avaya     ],
                microsoft:  [   citrix, vsphere   ],
                ...
                oracle:     [   hacmp, hana       ] }
    """

    def __init__(self,
                 d_terms: dict):
        """
        Created:
            23-Mar-2019
            craig.trim@ibm.com
        :param d_terms:
            a dictionary keyed by term type with Counter values
        """
        BaseObject.__init__(self, __name__)
        if not d_terms:
            raise MandatoryParamError("Dict")
        self.d_terms = d_terms

    @staticmethod
    def _terms(some_terms: list) -> set:
        s = set()
        while len(s) < 5:
            s.add(some_terms[randint(0, 99)])
        return s

    def _merge(self,
               some_keys: set) -> set:

        s = set()
        for k in some_keys:
            keys = set(list(self.d_terms[k]))
            s = s.union(keys)

        self.logger.debug("\n".join([
            "Merged Keys",
            "\tkeys: {}".format(some_keys),
            "\ttotal: {}".format(len(s))
        ]))

        return s

    def process(self):

        keys = set(self.d_terms)
        for k in self.d_terms:
            self.d_terms[k]["diff"] = self._merge(keys.difference({k}))

        for k in self.d_terms:
            a = set(list(self.d_terms[k]["counter"]))
            b = set(self.d_terms[k]["diff"])
            d = sorted(set(a.difference(b)))
