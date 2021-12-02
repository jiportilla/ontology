#!/usr/bin/env python
# -*- coding: UTF-8 -*-


from nltk.stem.snowball import SnowballStemmer

from base import BaseObject

stemmer = SnowballStemmer("english", ignore_stopwords=False)


class EntityDictGenerator(BaseObject):
    def __init__(self, some_df):
        """
        Updated:
            12-Apr-2017
            craig.trim@ibm.com
            *   renamed from "ProductDictGenerator"
        Updated:
            25-May-2017
            craig.trim@ibm.com
            *   created 'get_params' method
        Updated:
            2-Aug-2017
            craig.trim@ibm.com
            *   modify entity generation using provenance as a dictionary key
                <https://github.ibm.com/abacus-implementation/Abacus/issues/1721#issuecomment-3080923>
        Updated:
            19-Mar-2018
            craig.trim@ibm.com
            *   moved static methods to "private class"
            *   stemmer should only operate on unigrams
        Updated:
            21-Feb-2019
            craig.trim@ibm.com
            *   migrated from text
        Updated:
            26-Mar-2019
            craig.trim@ibm.com
            *   removed 'id' from attributes (only needed as key)
        :param some_df: 
        """
        BaseObject.__init__(self, __name__)
        self.df = some_df

    def generate_tuple_l2(self, tokens):
        t0 = self.stem(tokens[0])
        t1 = self.stem(tokens[1])

        if t0 != t1:
            return [(t0, t1)]
        return []

    def generate_tuple_l3(self, tokens):
        return [(
            self.stem(tokens[0]),
            self.stem(tokens[1]),
            self.stem(tokens[2]))]

    def generate_tuple_l4(self, tokens):
        return [(
            self.stem(tokens[0]),
            self.stem(tokens[1]),
            self.stem(tokens[2]),
            self.stem(tokens[3]))]

    def get_variation(self, some_token):
        if "+" in some_token:
            tmp = [x.lower().strip() for x in some_token.split("+")]

            if 2 == len(tmp):
                return self.generate_tuple_l2(tmp)
            elif 3 == len(tmp):
                return self.generate_tuple_l3(tmp)
            elif 4 == len(tmp):
                return self.generate_tuple_l4(tmp)

            raise ValueError(
                "Unrecognized Tuple (value = {0})".format(some_token))

        return [some_token]

    @staticmethod
    def add_to_set(the_set, the_value):
        if the_value is None:
            return

        if not isinstance(the_value, tuple):
            the_value = the_value.strip().lower()

        if len(the_value) == 0:
            return

        the_set.add(the_value)

    def get_expanded_variations(self, some_label, some_list):
        if some_list.startswith('"') and some_list.endswith('"'):
            some_list = some_list[1:len(some_list) - 1]

        the_set = set()

        if 0 == len(some_label.split(" ")):
            the_variant = stemmer.stem(some_label.lower())
            self.add_to_set(the_set, the_variant)
        else:
            self.add_to_set(the_set, some_label.lower())

        for token in some_list.split(","):
            try:
                for variant in self.get_variation(token):
                    self.add_to_set(the_set, variant)
            except ValueError:
                continue

        # return list(sorted(the_set))
        return list(the_set)

    @staticmethod
    def get_params(row):
        d = {}
        param = row["param"].strip()

        # default param to 'type=label'
        if 0 == len(param):
            key = row["type"].strip().lower()
            d[key] = row["label"].strip().lower()
            return d

        for p in param.split(","):
            tokens = [x.lower().strip() for x in p.split("=")]
            d[tokens[0]] = tokens[1]

        return d

    def process(self):

        the_master_dict = {}
        prov_list = filter(lambda x: len(x) > 0, self.df.prov.unique())

        for prov in prov_list:
            df2 = self.df[self.df.prov == prov]

            for i, row in df2.iterrows():
                the_label = row["label"].strip()
                the_id = self.key(the_label)
                the_type = self.key(row["type"].strip())
                the_params = self.get_params(row)
                the_scope = row["scope"].strip()

                the_variants = self.get_expanded_variations(
                    some_label=the_label,
                    some_list=row["variations"].strip())

                the_master_dict[the_id] = {
                    # "id": the_id,
                    "label": the_label,
                    "type": the_type,
                    # "params": the_params,
                    "variations": the_variants
                    # "scope": the_scope
                }

        return the_master_dict

    @staticmethod
    def key(value):
        return value.replace(" ", "_").lower().strip()

    @staticmethod
    def stem(some_token):
        """
        Purpose:
            perform stemming operation using Snowball

            Rules:
            1.  Only stem unigrams
                non-unigrams contain 1..* whitespace tokens
            2.  Do not stem patterns
                patterns start with "$"
            3.  Do not stem pre-compounded tokens
                e.g. "contract_number" is pre-compounded in a prior stage
                running this through the stemmer would generate
                    "contract_numb"
        """
        if " " in some_token:
            return some_token
        if "_" in some_token:
            return some_token
        if some_token.startswith("$"):
            return some_token
        # entity matching is supposed to have both “exact” and “partial” matching but
        # perhaps the partial matching was removed by accident so commenting out this line to stop the stemming
        # return stemmer.stem(some_token.strip())
        return some_token
