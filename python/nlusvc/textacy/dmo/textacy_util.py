#!/usr/bin/env python
# -*- coding: UTF-8 -*-


import logging
import time

import spacy
import textacy
import textacy.extract
import textacy.keyterms
from textacy import Corpus

from datadict import LoadStopWords


class TextactyUtils:
    """
    """

    logger = logging.getLogger(__name__)
    stopwords = LoadStopWords().load()

    @classmethod
    def spacy_model(cls):
        return spacy.load("en_core_web_sm")

    @classmethod
    def lines_to_doc(cls,
                     some_lines: list,
                     remove_stopwords=True) -> textacy.doc:
        """
        :param some_lines:
        :param remove_stopwords:
        :return:
            lines
        """
        _lines = []
        some_lines = [x.lower().strip() for x in some_lines]

        for line in some_lines:
            _line = []
            for token in line.split(" "):
                if remove_stopwords:
                    if token not in cls.stopwords:
                        _line.append(token)
                else:
                    _line.append(token)
            _lines.append(" ".join(_line))

        svcresult = " ".join(_lines)
        return textacy.make_spacy_doc(svcresult)

    @classmethod
    def doc(cls,
            spacy_model,
            some_text: str,
            no_numbers=True,
            is_debug=False) -> textacy.doc:
        """ use textacy to preprocess the text prior to creating a doc
        :return:
            a textacy doc
        """

        original_text = some_text

        some_text = textacy.preprocess_text(some_text,
                                            # fix_unicode=True,
                                            lowercase=True,
                                            no_urls=True,
                                            no_emails=True,
                                            no_phone_numbers=True,
                                            no_numbers=no_numbers,
                                            no_currency_symbols=True,
                                            no_punct=True,
                                            no_contractions=True,
                                            no_accents=True)

        if no_numbers:
            # textact replaces numbers and years with 'numb' and 'year' respectively
            # for topic modeling these are best removed
            some_text = some_text.replace("number", " ")
            some_text = some_text.replace("numb", " ")
            some_text = some_text.replace("years", " ")
            some_text = some_text.replace("year", " ")

        if is_debug:
            cls.logger.debug("\n".join([
                "Textacy Preprocessed Text",
                "\toriginal: {}".format(original_text),
                "\tpreprocessed: {}".format(some_text)
            ]))

        return textacy.make_spacy_doc(spacy_model(some_text))

    @classmethod
    def corpus(cls,
               spacy_model,
               some_values: list,
               is_debug=True) -> Corpus:
        """ A textacy.Corpus is an ordered collection of textacy.Docs,
            all processed by the same spacy language pipeline.
        :return:
            a textacy corpus of docs
        """
        start = time.time()

        docs = []
        [docs.append(cls.doc(spacy_model,
                             x)) for x in some_values]
        corpus = textacy.Corpus(spacy_model)
        corpus.docs = docs

        if is_debug:
            cls.logger.debug("\n".join([
                "Generated Corpus",
                "\tTotal Documents: {}".format(corpus.n_docs),
                "\tTotal Sentences: {}".format(corpus.n_sents),
                "\tTotal Tokens: {}".format(corpus.n_tokens),
                "\tTotal Time: {}".format(time.time() - start)
            ]))

        return corpus
