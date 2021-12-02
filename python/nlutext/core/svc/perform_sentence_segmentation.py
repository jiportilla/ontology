#!/usr/bin/env python
# -*- coding: UTF-8 -*-


import re

from spacy.lang.en import English

from base import BaseObject


class PerformSentenceSegmentation(BaseObject):
    """ Sentence Segmentation """

    # common patterns that potentially mark sentence boundaries
    __patterns = ['\t', '-', '•', '  ', ':', '..', '...', '',
                  '', '', '', ',', ';', '*', '|', '•\t',
                  '•', '', '·']  # GIT-759-13925326
    __nlp = None

    def __init__(self,
                 is_debug: bool = False):
        """
        Created:
            18-Mar-2019
            craig.trim@ibm.com
        Updated:
            12-Apr-2019
            craig.trim@ibm.com
            *   add logging and stopwatch to _segmenter
        Updated:
            16-Aug-2019
            craig.trim@ibm.com
            *   added in pursuit of
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/759
        Updated:
            10-Sept-2019
            craig.trim@ibm.com
            *   removed '.' from patterns - this caused recursive defect
                https://github.ibm.com/-cdo/unstructured-analytics/issues/894
        Updated:
            13-Oct-2019
            craig.trim@ibm.com
            *   add threshold to pattern replacement
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1098#issuecomment-15262587
        Updated:
            31-Oct-2019
            craig.trim@ibm.com
            *   treat double-space as a delimiter in pursuit of
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1228
        Updated:
            15-Nov-2019
            xavier.verges@es.ibm.com
            *   remove warning from regexp
            *   cache spacy nlp pipeline creation
        Updated:
            15-Nov-2019
            craig.trim@ibm.com
            *   updates in support of
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1378
        Updated:
            26-Nov-2019
            craig.trim@ibm.com
            *   add warning on input to process method in pursuit of
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1452
        """
        BaseObject.__init__(self, __name__)

        self.is_debug = is_debug
        self.segmenter = self._segmenter()

    @classmethod
    def _segmenter(cls) -> English:
        if not cls.__nlp:
            cls.__nlp = English()
            sentencizer = cls.__nlp.create_pipe("sentencizer")
            cls.__nlp.add_pipe(sentencizer)

        return cls.__nlp

    @staticmethod
    def _preprocess(some_input_text: str) -> str:
        """
        Purpose:
            prevent numbered bullet points from triggering sentence detection
        :param some_input_text:
            any input text
        :return:
            preprocessed input text
        """
        for i in range(0, 9):
            key = "{}.".format(i)
            if key in some_input_text:
                some_input_text = some_input_text.replace(key, "")

        if "  " in some_input_text:
            some_input_text = some_input_text.replace("  ", " ")

        # the replacement routine above leaves double '..' in the text
        # this replacement will solve that
        while ".." in some_input_text:  # GIT-1378-10847186
            some_input_text = some_input_text.replace("..", ".")
        while ". . " in some_input_text:  # GIT-1378-16068423
            some_input_text = some_input_text.replace(". . ", ".")

        return some_input_text

    @staticmethod
    def _commas_to_periods(input_text: str):
        """
        Purpose:
            Take a CSV list and transform to sentences
        Sample Input:
            input_text =   "Dialog and Chat Bots, Deep Learning, Machine Learning, Data Mining, Enterprise Architecture,
                            Data Analytics, Statistics, PySpark, Python, XSLT, Hadoop, UML, Docker, Jenkins, tensorflow,
                            Java Enterprise Edition, DB2, Spark, Natural Language Processing, Data Science, Ontologies,
                            WEKA, OWL, W3C, Technical Presentations, Provenance, RDFS, Solr, Elasticsearch, Lucene, word2vec,
                            NLTK, spaCy, Cloud Computing, AWS, Google Cloud Compute, IBM Bluemix, NLC, Ubuntu Debian Linux,
                            Red Hat Linux, CentOS"
        Metrics:
            total-commas:   44
            total-len:      874
            ratio:          0.0503
        :param input_text:
        :return:
        """
        total_len = len(input_text)
        total_commas = input_text.count(",")
        if total_commas / total_len > 0.04:
            return input_text.replace(',', '.')

        return input_text

    def process(self,
                some_input_text: str,
                remove_wiki_references: bool = True) -> list:
        """
        Purpose:
            Perform Sentence Segmentation
        :param some_input_text:
            any input text
        :param remove_wiki_references:
            True        remove wikipedia style references
            Sample Input:
                "A blockchain,[1][2][3] originally block chain,[4][5] is a growing list of records"
            Sample Output:
                "A blockchain, originally block chain, is a growing list of records"
        :return:
            a list of 0-or-More sentences
        """

        if type(some_input_text) != str:  # GIT-1452-16247537
            self.logger.warning(f"Invalid Input Text: {some_input_text}")
            return []

        some_input_text = self._commas_to_periods(input_text=some_input_text)  # GIT-1365-16009049

        if remove_wiki_references:
            if '[' and ']' in some_input_text:
                some_input_text = re.sub(r'\[[^\]]+\]', '', some_input_text)

        some_input_text = some_input_text.replace('   ', '  ')  # eliminate triple-space
        some_input_text = some_input_text.replace('  ', '. ')  # treat double-space as delimiter
        # need to integrate https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1229

        if "." not in some_input_text:
            return [some_input_text]

        some_input_text = self._preprocess(some_input_text)

        doc = self.segmenter(some_input_text)

        sentences = [str(sent) for sent in doc.sents]
        sentences = [sent for sent in sentences if
                     sent and len(sent) and sent != 'None']

        return sentences
