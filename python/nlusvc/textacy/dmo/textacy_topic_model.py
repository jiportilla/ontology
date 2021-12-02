#!/usr/bin/env python
# -*- coding: UTF-8 -*-


import pprint
import time

import spacy
from scipy.sparse.csr import csr_matrix
from textacy import Corpus
from textacy.tm import TopicModel
from textacy.vsm import Vectorizer

from base import BaseObject
from base import MandatoryParamError


class TextacyTopicModeler(BaseObject):
    """
    https://chartbeat-labs.github.io/textacy/getting_started/quickstart.html#analyze-a-corpus
    """

    _nlp = spacy.load("en_core_web_sm",
                      disable=('parser', 'tagger'))

    _topic_model_types = ['nmf', 'lda', 'lsa']

    def __init__(self,
                 some_values: list,
                 number_of_topics=10,
                 terms_per_topic=10,
                 is_debug=True):
        """
        Created:
            3-Apr-2019
            craig.trim@ibm.com
        """
        BaseObject.__init__(self, __name__)
        if not some_values:
            raise MandatoryParamError("Input Values")

        self.is_debug = is_debug
        self.values = some_values
        self.terms_per_topic = terms_per_topic
        self.number_of_topics = number_of_topics

    @staticmethod
    def _vectorizer() -> Vectorizer:
        return Vectorizer(tf_type='linear',
                          apply_idf=True,
                          idf_type='smooth',
                          norm='l2',
                          min_df=2,
                          max_df=0.95)

    def _doc_term_matrix(self,
                         vectorizer: Vectorizer,
                         corpus: Corpus) -> csr_matrix:
        start = time.time()
        doc_term_matrix = vectorizer.fit_transform((doc.to_terms_list(ngrams=1,
                                                                      named_entities=True,
                                                                      as_strings=True)
                                                    for doc in corpus))

        if self.is_debug:
            self.logger.debug("\n".join([
                "Generated Document/Term Matrix",
                "\trepr: {}".format(repr(doc_term_matrix)),
                "\tTotal Time: {}".format(time.time() - start)
            ]))

        return doc_term_matrix

    def _topic_model(self,
                     doc_term_matrix: csr_matrix,
                     topic_model_type='nmf') -> TopicModel:
        start = time.time()

        if topic_model_type not in self._topic_model_types:
            raise NotImplementedError("\n".join([
                "Topic Model Type Not Recognized",
                "\tname: {}".format(topic_model_type)
            ]))

        model = TopicModel(topic_model_type,
                           n_topics=self.number_of_topics)

        try:
            model.fit(doc_term_matrix)
        except IndexError as e:
            raise ValueError("\n".join([
                "Model Fit Error",
                "\t{}".format(str(e)),
                "\tTry decreasing topic-size and/or terms-per-topic"
            ]))

        doc_topic_matrix = model.transform(doc_term_matrix)

        if self.is_debug:
            self.logger.debug("\n".join([
                "Generated Topic Model",
                "\tShape: {}".format(doc_topic_matrix.shape),
                "\tTotal Time: {}".format(time.time() - start)
            ]))

        return model

    def _result_set(self,
                    vectorizer: Vectorizer,
                    model: TopicModel) -> list:
        l_results = []
        for topic_idx, top_terms in model.top_topic_terms(vectorizer.id_to_term,
                                                          top_n=self.terms_per_topic):
            l_results.append({
                "topic_idx": topic_idx,
                "top_terms": top_terms
            })

        return l_results

    def process(self) -> list:
        from nlusvc.textacy.dmo import TextactyUtils

        start = time.time()
        corpus = TextactyUtils.corpus(spacy_model=self._nlp,
                                      some_values=self.values,
                                      is_debug=self.is_debug)

        vectorizer = self._vectorizer()
        doc_term_matrix = self._doc_term_matrix(vectorizer,
                                                corpus)
        model = self._topic_model(doc_term_matrix)
        results = self._result_set(vectorizer,
                                   model)

        if self.is_debug:
            self.logger.debug("\n".join([
                "Topic Modeling Complete",
                "\tTotal Time: {}".format(time.time() - start),
                pprint.pformat(results)
            ]))

        return results
