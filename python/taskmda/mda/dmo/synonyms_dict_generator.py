#!/usr/bin/env python
# -*- coding: UTF-8 -*-


import re

from base import BaseObject


class SynonymsDictGenerator(BaseObject):

    def __init__(self, some_df):
        """
        Updated:
            21-Feb-2019
            craig.trim@ibm.com
            *   migrated to text
        Updated:
            15-Jul-2019
            craig.trim@ibm.com
            *   defect fix when no explicit variations are present
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/437
        Updated:
            22-Jul-2019
            anassar@us.ibm.com
            *   add canonical form for underscore insensitive
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/465
        Updated:
            08-Nov-2019
            xavier.verges@es.ibm.com
            *   split variations using a regular expression, so that embedded regexps can include commas
        Updated:
            21-Nov-2019
            craig.trim@ibm.com
            *   support expansion pattern
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1424#issue-10910179
        :param some_df:
        """
        BaseObject.__init__(self, __name__)
        self.dict = {}
        self.df = some_df

    def get_dict(self):
        return self.dict

    def normalize_variations(self,
                             variants: set) -> set:
        the_set = set()

        variants = [x.lower().strip() for x in variants]
        variants = [x for x in variants if x and len(x)]

        for variant in variants:

            # support expansion pattern
            if '!' in variant and not variant.endswith('!'):  # GIT-1424-10910179

                tokens = [x.strip() for x in variant.split('!')]
                tokens = [x for x in tokens if x and len(x)]
                if len(tokens) != 2:
                    self.logger.error('\n'.join([
                        "Unexpected Expansion Pattern",
                        f"\tVariant: {variant}",
                        f"\tTokens: {tokens}"]))
                    raise ValueError()

                the_set.add(f"{tokens[0]} {tokens[1]}")
                the_set.add(f"{tokens[1]} {tokens[0]}")

            else:  # standard variant
                the_set.add(variant)

        return the_set

    @staticmethod
    def normalize_canonical_form(some_canonical_form):
        return some_canonical_form.lower().strip()

    @staticmethod
    def normalize_dictionary(some_dict):
        normalized_dict = {}
        for key in some_dict:
            normalized_dict[key] = [x.strip() for x in sorted(some_dict[key]) if x]

        return normalized_dict

    def process(self) -> dict:

        # https://www.debuggex.com/r/KIWZ4tGTLYuLAcyR
        split_by_comma_keeping_regexps = re.compile(r'(?:[^,]+\[.*\])[^,]*|[^,]+')

        canon_by_variant = {}
        for i, row in self.df.iterrows():

            def _variations() -> set:
                v = split_by_comma_keeping_regexps.findall(row["variants"])
                v = [x.strip() for x in v if x]
                if 0 < len(the_canon) and "_" in the_canon:
                    v.append(the_canon.replace("_", " "))
                under_score_insensitive = list()
                for t in v:
                    if "_" in t:
                        under_score_insensitive.append(str(t).replace("_", " ").strip())
                return set(sorted(v + under_score_insensitive))

            the_canon = self.normalize_canonical_form(row["canon"])
            the_variants = self.normalize_variations(_variations())

            if 0 == len(the_canon):
                continue
            elif 0 == len(the_variants):
                the_variants = set([the_canon.replace("_", " ")])

            if the_canon not in self.dict:
                self.dict[the_canon] = set()

            self.dict[the_canon] = set(self.dict[the_canon]).union(set(the_variants))
            for variant in self.dict[the_canon]:
                if variant not in canon_by_variant:
                    canon_by_variant[variant] = []
                canon_by_variant[variant].append(the_canon)

        self.dict = self.normalize_dictionary(self.dict)
        warn = False
        count = 1
        for variant in sorted(canon_by_variant):
            canons = canon_by_variant[variant]
            if len(canons) != 1:
                variant_without_blanks = variant.replace(' ', '_')
                canons = [x for x in canons if x != variant_without_blanks]
            if len(canons) != 1:
                warn = True
                print(f'Non-univocal synonym {count} {variant}:{canons}')
                count += 1

        return self.dict
