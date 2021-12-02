#!/usr/bin/env python
# -*- coding: UTF-8 -*-


from nltk.stem.snowball import SnowballStemmer

from base import BaseObject

stemmer = SnowballStemmer("english", ignore_stopwords=False)


class EntityNgramDictGenerator(BaseObject):

    def __init__(self,
                 is_debug=False):
        """
        Updated:
            12-Apr-2017
            craig.trim@ibm.com
            *   renamed from "ProductNgramDictGenerator"
        Updated:
            21-Feb-2019
            craig.trim@ibm.com
            *   migrated to text
        Updated:
            15-Oct-2019
            xavier.verges@es.ibm.com
            *   sorted lists to make changes in the generated files easier to track
        """
        BaseObject.__init__(self, __name__)
        self.is_debug = is_debug
        self.dict = self._initialize_dict()

    @staticmethod
    def _initialize_dict() -> dict:
        return {
            "gram-1": set(),
            "gram-2": set(),
            "gram-3": set(),
            "gram-4": set(),
            "gram-5": set()
        }

    def normalize_ngram_dict(self):
        the_normalized_dict = {
            "gram-1": sorted(list(self.dict["gram-1"])),
            "gram-2": sorted(list(self.dict["gram-2"])),
            "gram-3": sorted(list(self.dict["gram-3"])),
            "gram-4": sorted(list(self.dict["gram-4"])),
            "gram-5": sorted(list(self.dict["gram-5"]))
        }

        self.dict = the_normalized_dict

    @staticmethod
    def get_tokens(some_label, some_variations):

        the_token_set = set()
        the_token_set.add(some_label.lower().strip())

        the_token_set = the_token_set.union(set(some_variations))

        return list(the_token_set)

    def update_ngrams_dict(self, some_tokens):
        for some_token in some_tokens:

            if isinstance(some_token, tuple):
                continue

            some_token = some_token.strip()
            if "+" in some_token:
                continue
            if len(some_token) < 2:
                continue

            tmp = some_token.split(" ")
            if 1 == len(tmp):
                self.dict["gram-1"].add(some_token)
            elif 2 == len(tmp):
                self.dict["gram-2"].add(some_token)
            elif 3 == len(tmp):
                self.dict["gram-3"].add(some_token)
            elif 4 == len(tmp):
                self.dict["gram-4"].add(some_token)
            elif 5 == len(tmp):
                self.dict["gram-5"].add(some_token)
            else:
                if self.is_debug:
                    self.logger.warning("\n".join([
                        "Token Length Exceeds n-Gram Comp Size",
                        "\tlen = {}".format(len(tmp)),
                        "\tvalue = {}".format(some_token)
                    ]))

    def process(self,
                some_labels: list,
                some_patterns: list) -> dict:

        def _normalize_labels() -> list:
            labels = set()
            [labels.add(x.replace("_", " ")) for x in some_labels]
            return sorted(labels)

        def _normalize_patterns() -> list:
            patterns = set()
            [[patterns.add(y.replace("_", " ")) for y in x] for x in some_patterns]
            return sorted(patterns)

        self.update_ngrams_dict(_normalize_labels())
        self.update_ngrams_dict(_normalize_patterns())

        self.normalize_ngram_dict()

        return self.dict
