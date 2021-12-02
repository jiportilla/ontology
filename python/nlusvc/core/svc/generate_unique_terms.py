#!/usr/bin/env python
# -*- coding: UTF-8 -*-


from base import BaseObject
from base import MandatoryParamError
from nlusvc.core.dmo import FreeformTextQuery
from nlusvc.core.dmo import FrequentTermExtractor
from nlusvc.core.dmo import TermCluster


class GenerateUniqueTerms(BaseObject):
    """
    """

    def __init__(self,
                 d_query: dict):
        """
        Created:
            23-Mar-2019
            craig.trim@ibm.com
        :param d_query:
            a dictionary of query strings to run service against

            sample input:
                {   "redhat":       [   redhat, rhsa, centos, ..., fedora   ],
                    "cisco":        [   cisco, cisco_router ..., avaya      ],
                    "mainframe":    [   xseries, pseries, ..., aix          ],
                    ...
                    "microsoft":    [   microsoft, windows, ..., zune       ]   }
        """
        BaseObject.__init__(self, __name__)
        if not d_query:
            raise MandatoryParamError("Query Dictionary")

        self.d_query = self._cleanse(d_query)
        self.frequent_term_extractor = FrequentTermExtractor()

    @staticmethod
    def _cleanse(some_query_dict: dict) -> dict:
        """
        :param some_query_dict:
            a dictionary where each value is a list comprised of unigrams
        :return:
            the same list of lists with cleansed (and validated) values as needed
        """
        for k in some_query_dict:
            some_query_dict[k] = sorted([x.lower().strip()
                                         for x in some_query_dict[k] if x])
        return some_query_dict

    def _term_frequency(self) -> dict:
        d_words = {}
        for key in self.d_query:
            d_terms = FreeformTextQuery(self.d_query[key]).process()
            d_words[key] = {
                "counter": self.frequent_term_extractor.process(d_terms)
            }

        return d_words

    def process(self):

        d_words = self._term_frequency()

        import pprint
        pprint.pprint(d_words)

        TermCluster(d_words).process()


if __name__ == "__main__":
    the_d_query = {
        "data": ["data", "science", "python", "natural_language_processing", "vector", "machine_learning"],
        "cloud": ["infrastructure_as_a_service", "paas", "saas"],
        "mainframe": ["aix", "pseries", "xseries", "mainframe"],
        "oracle": ["oracle", "oracle_certification", "oracle_router"]}
    GenerateUniqueTerms(the_d_query).process()
