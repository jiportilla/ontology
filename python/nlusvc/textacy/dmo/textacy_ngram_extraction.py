#!/usr/bin/env python
# -*- coding: UTF-8 -*-


import spacy
import textacy.io
import textacy.keyterms
from textacy.vsm.vectorizers import GroupVectorizer

from base import BaseObject
from datadict import FindInput


class TextacyNgramExtraction(BaseObject):
    """
    https://chartbeat-labs.github.io/textacy/api_reference.html#module-textacy.extract
    """

    _nlp = spacy.load("en_core_web_sm",
                      disable=('parser', 'tagger'))

    _topic_model_types = ['nmf', 'lda', 'lsa']

    def __init__(self,
                 is_debug=True):
        """
        Created:
            11-Apr-2019
            craig.trim@ibm.com
        Updated:
            23-Oct-2019
            craig.trim@ibm.com
            *   functional updates to maintain operation
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1186
        """
        BaseObject.__init__(self, __name__)

        self.is_debug = is_debug
        self.input_finder = FindInput()

    def ngrams(self,
               some_lines: list,
               n: int) -> list:
        from nlusvc.textacy.dmo import TextactyUtils

        _nlp = TextactyUtils.spacy_model()
        corpus = TextactyUtils.corpus(spacy_model=_nlp,
                                      some_values=some_lines,
                                      is_debug=self.is_debug)

        def _to_terms_list(a_doc: textacy.doc):
            return list(textacy.extract.ngrams(a_doc, n))
            # return a_doc.to_terms_list(ngrams=n,
            #                            named_entities=True,
            #                            as_strings=True)

        tokenized_docs, groups = textacy.io.unzip(((_to_terms_list(doc), "group") for doc in corpus))

        vectorizer = GroupVectorizer(apply_idf=True,
                                     idf_type='smooth',
                                     norm='l2',
                                     min_df=1,
                                     max_df=1.95)

        vectorizer.fit_transform(tokenized_docs, groups)

        terms = vectorizer.terms_list
        terms = [str(x.text).lower().strip() for x in terms if x]
        terms = [x for x in terms if not self.input_finder.exists(x)]

        return terms


if __name__ == "__main__":
    lines = ["AUTO-TRANSLATION OF SOURCE STRINGS IN GLOBAL VERIFICATION TESTING IN A FUNCTIONAL TESTING TOOL"]
    print(TextacyNgramExtraction(is_debug=True).ngrams(some_lines=lines, n=2))
