#!/usr/bin/env python
# -*- coding: UTF-8 -*-


import codecs
import os

from base import BaseObject
from datadict import the_parents_dict


class CleanGoWords(BaseObject):
    """ """

    def __init__(self):
        """
        Updated:
            2-Apr-2019
            craig.trim@ibm.com
        """
        BaseObject.__init__(self, __name__)
        self.labels = [x for x in the_parents_dict
                       if "DomainTerm" not in the_parents_dict[x]]

    @staticmethod
    def _lines() -> list:
        path = os.path.join(os.environ["CODE_BASE"],
                            "resources/nlu/other/gowords_kb.csv")
        target = codecs.open(path, "r", encoding="utf-8")
        lines = target.readlines()
        target.close()

        return lines

    @staticmethod
    def _equivalent_terms(gowords: list,
                          labels: list) -> list:
        s = set()
        for label in labels:
            for goword in gowords:
                if label == goword:
                    s.add(label)

        return sorted(s)

    @staticmethod
    def _partial_match_unigrams(gowords: list,
                                labels: list) -> list:
        valid = ['design, designer',
                 'engineer, engineering',
                 'file, filenet',
                 'power, powerha',
                 'power, powersc',
                 'power, powervm',
                 'total, totalstorage',
                 'web, websphere']

        s = set()
        for label in labels:
            for goword in gowords:
                if label.startswith(goword):

                    if " " not in label and " " not in goword:
                        s.add(", ".join(sorted({label, goword})))

        return [x for x in sorted(s)
                if x not in valid]

    def process(self):
        gowords = [x.lower().strip() for x in self._lines() if x]
        labels = [x.lower().strip() for x in self.labels if x]

        equivalent_terms = self._equivalent_terms(gowords,
                                                  labels)
        if len(equivalent_terms):
            self.logger.warning("\n".join([
                "Equivalent Terms Found",
                "\n".join(equivalent_terms)
            ]))
        else:
            self.logger.debug("\n".join([
                "Congratulations! No Equivalent Terms Found"
            ]))

        if len(equivalent_terms):  # early break; nothing else matters
            return

        partial_match_unigrams = self._partial_match_unigrams(gowords,
                                                              labels)
        if len(partial_match_unigrams):
            self.logger.warning("\n".join([
                "Partially Matched Unigrams Found",
                "\n".join(partial_match_unigrams)
            ]))
        else:
            self.logger.debug("\n".join([
                "Congratulations! No Partially Matched Unigrams Found"
            ]))

        # elif label.startswith(goword):
        #     print(label, " --> ", goword)
        # elif goword.startswith(label):
        #     print(goword, " --> ", label)


def main():
    CleanGoWords().process()


if __name__ == "__main__":
    main()
