# !/usr/bin/env python
# -*- coding: UTF-8 -*-


from base import BaseObject

PROV_WILDCARD = "entity"


class FindEntityNgrams(BaseObject):
    """ act as a controller in front of all entity ngram dictionaries """

    def __init__(self,
                 ontology_name: str = 'base',
                 is_debug: bool = False):
        """
        Created:
            27-Jul-2017
            craig.trim@ibm.com
            *   initial methods refactored out of 'entity-json-adapter'
        Updated:
            27-Sep-2017
            craig.trim@ibm.com
            *   migrated to abacus-att repo
                removed env var
        Updated:
            21-Feb-2019
            craig.trim@ibm.com
            *   migrated to text
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

        self.ngrams = _loader.taxonomy().ngrams()

    def by_level(self, n):
        if 1 == n:
            return self.ngrams["gram-1"]
        elif 2 == n:
            return self.ngrams["gram-2"]
        elif 3 == n:
            return self.ngrams["gram-3"]
        elif 4 == n:
            return self.ngrams["gram-4"]
        elif 5 == n:
            return self.ngrams["gram-5"]

        raise ValueError("Unexpected Gram Level (level = {0})".format(n))
