#!/usr/bin/env python
# -*- coding: UTF-8 -*-


"""
    Purpose:
        matches language formations in the synonyms file to entities.yml

    Rationale:
        the synonyms file may define an entity like 'support_request_problem'
        and the entities.yml file does nothing with this data

        this means the the entry in the synonyms file is not serving a useful purpose
        and either
        1.  an entry needs to be created in entities.yml
        2.  the synonyms entry should be removed
"""

import os
from collections import Counter

from nltk import ngrams

from base import BaseObject
from base import FileIO
from datadict import the_stopwords_dict


class NgramTaxonomyGenerator(BaseObject):

    def __init__(self):
        """
        Created:
            14-Mar-2019
            craig.trim@ibm.com
            *   refactored out of a script named 'impute-ngram-taxonomy'
        """
        BaseObject.__init__(self, __name__)

        self.stopwords = self._load_stopwords()

    @staticmethod
    def _load_stopwords():
        path = os.path.join(os.environ["GTS_BASE"],
                            os.environ["STOP_WORDS_LEARNING"])
        lines = FileIO.file_to_lines(path, use_sort=False)
        lines += the_stopwords_dict

        lines = sorted(set(lines))
        return lines

    def _ngrams(self,
                c: Counter, text, level):
        def _cleanse(some_text: str) -> str:
            some_text = some_text.replace(".", " ")
            some_text = some_text.replace("-", " ")
            some_text = some_text.replace(",", " ")
            some_text = some_text.replace("/", "_")
            some_text = some_text.replace("&", "and")
            return some_text

        def _valid(x) -> bool:
            if type(x) == tuple:
                for i in range(0, len(x)):
                    if x[i] in self.stopwords:
                        return False
            return True

        text = _cleanse(text)
        tokens = [x for x in text.split() if x and x.lower().strip() not in self.stopwords]
        tokens = [_cleanse(x) for x in tokens]
        tokens = [x for x in ngrams(tokens, level) if x and len(x) > 0]
        tokens = [x for x in tokens if _valid(x)]
        tokens = [" ".join(x) for x in tokens if x]
        [c.update({x: 1}) for x in tokens if x != [] and len(x) > 0]

        return c

    def process(self,
                some_text: list,
                most_common_threshold: int = 250):

        def c_grams(level):
            c = Counter()
            [self._ngrams(c, x, level) for x in some_text]
            return c

        def _most_common(c: Counter,
                         n: int,
                         a_filter: str):
            return sorted([x for x in c.most_common(n) if a_filter in x[0]])

        unigrams = c_grams(1)
        bigrams = c_grams(2)
        trigrams = c_grams(3)

        def _impute_taxonomy(term):

            f_unigrams = _most_common(unigrams, most_common_threshold, term)
            print(f_unigrams)

            f_bigrams = _most_common(bigrams, most_common_threshold, term)
            for f_bigram in f_bigrams:
                print("\t", f_bigram)

                f_trigrams = _most_common(trigrams, most_common_threshold, f_bigram[0])
                for f_trigram in f_trigrams:
                    print("\t\t", f_trigram)

        for x in unigrams.most_common(50):
            print("\n\n------ ", x[0])
            _impute_taxonomy(x[0])


if __name__ == "__main__":
    tl = [
        "This specialty applies their project management skills supporting Identity & Access Management projects."
    ]
    NgramTaxonomyGenerator().process(tl, most_common_threshold=500)
