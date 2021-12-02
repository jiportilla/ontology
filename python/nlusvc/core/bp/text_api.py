#!/usr/bin/env python
# -*- coding: UTF-8 -*-


from typing import Optional

from pandas import DataFrame

from base import BaseObject
from base import DataTypeError
from datamongo import BaseMongoClient
from nlutext import TextParser


class TextAPI(BaseObject):
    """ one-stop API shop for generic Text Wrangling
    """

    _pos_tagger = None
    _pos_analyzer = None
    _sentencizer = None
    _parser = None

    def __init__(self,
                 ontology_name: str = 'base',
                 is_debug: bool = False):
        """
        Created:
            27-Jun-2019
            craig.trim@ibm.com
            *   act primarily as a wrapper around textacy
                add additional functionality
        Updated:
            9-Jul-2019
            craig.trim@ibm.com
            *   add 'part-of-speech-tagging' and
                    'part-of-speech-analysis' methods
        Updated:
            17-Oct-2019
            craig.trim@ibm.com
            *   add 'has-match' function
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1142#issuecomment-15377548
        Updated:
            18-Nov-2019
            craig.trim@ibm.com
            *   update displacy-spans function
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1399
        Updated:
            13-Dec-2019
            craig.trim@ibm.com
            *   load dictionaries by ontology name
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1582
        """
        BaseObject.__init__(self, __name__)
        self.is_debug = is_debug
        self._ontology_name = ontology_name

        if self.is_debug:
            self.logger.debug('\n'.join([
                "Instantiated TextAPI",
                f"\tOntology Name: {ontology_name}"]))

    def coords(self,
               input_text: str,
               entity_text: str) -> Optional[dict]:
        """
        Purpose:
            Extract the (x,y) Coordinates of an Entity in the Input Text
        Sample Input:
            Input Text:
                "and the cytotoxic activity of natural killer (NK) cells"
            Entity Text:
                "Natural Killer Cell"
        Sample Output:
            {   'x': 30, y: '55',
                'substring': 'natural killer (NK) cells' }
        :param input_text:
            any input text in an original state (non-normalized)
        :param entity_text:
            any extracted (Ontology) entity
        :return:
            a dictionary of the (x,y) coordinates
        """
        from nlusvc.coords.svc import PerformCoordExtraction

        svc = PerformCoordExtraction(is_debug=self.is_debug,
                                     ontology_name=self._ontology_name)

        return svc.process(input_text=input_text,
                           entity_text=entity_text)

    def has_match(self,
                  a_token: str,
                  some_text: str) -> bool:
        """
        Purpose:
            Perform simple text string matching
        :param a_token:
            any unstructured input text or token
        :param some_text:
            any unstructured input text or token
        :return:
            True        if the incoming token is found in the incoming text
                        only exact matches are considered
            False       no match found
        """
        from nlusvc.core.svc import TextStringMatcher
        return TextStringMatcher(a_token=a_token,
                                 some_text=some_text,
                                 is_debug=self.is_debug).process()

    def parse(self,
              input_text: str) -> DataFrame:
        """
        Sample Input:
            'Security Engineering on AWS'
        Sample Output:
            +----+-----------------------------+-----------------------------+-------------+
            |    | InputText                   | NormalizedText              | Tag         |
            |----+-----------------------------+-----------------------------+-------------|
            |  0 | Security Engineering on AWS | security engineering on aws | AWS         |
            |  1 | Security Engineering on AWS | security engineering on aws | Engineering |
            |  2 | Security Engineering on AWS | security engineering on aws | Security    |
            +----+-----------------------------+-----------------------------+-------------+
        :param input_text:
            any input text of any length
        :return:
            a results DataFrame
        """
        if not self._parser:
            self._parser = TextParser(is_debug=self.is_debug,
                                      ontology_name=self._ontology_name)
        return self._parser.process(original_ups=input_text,
                                    as_dataframe=True,
                                    use_profiler=self.is_debug)

    def part_of_speech_tagging(self,
                               some_input: str or list) -> DataFrame or list:
        """
        perform Part-of-Speech tagging
        :param some_input:
            any unstructured string (or list of unstructured strings)
        :return:
            a Pandas Dataframe of the output

            Given this input
                "the quick brown fox"

            Return this output:
                +----+--------------+-----------+---------+----------------+---------+------------+-------+
                |    | Dependency   | IsAlpha   | Lemma   | PartOfSpeech   | Shape   | Stopword   | Tag   |
                |----+--------------+-----------+---------+----------------+---------+------------+-------|
                |  0 | det          | True      | the     | DET            | Xxx     | False      | DT    |
                |  1 | amod         | True      | quick   | ADJ            | xxxx    | False      | JJ    |
                |  2 | amod         | True      | brown   | ADJ            | xxxx    | False      | JJ    |
                |  3 | ROOT         | True      | fox     | NOUN           | xxx     | False      | NN    |
                +----+--------------+-----------+---------+----------------+---------+------------+-------+
        """
        from nlusvc.core.svc import PerformPosTagging
        if not self._pos_tagger:
            self._pos_tagger = PerformPosTagging(is_debug=self.is_debug)
        return self._pos_tagger.process(some_input)

    def part_of_speech_analysis(self,
                                some_input: DataFrame or list) -> DataFrame:
        """

        :param some_input:
            a part-of-speech DataFrame generated from 'part-of-speech-tagging' (or list of these DataFrames)

            Sample Input:
                +----+--------------+-----------+---------+----------------+---------+------------+-------+
                |    | Dependency   | IsAlpha   | Lemma   | PartOfSpeech   | Shape   | Stopword   | Tag   |
                |----+--------------+-----------+---------+----------------+---------+------------+-------|
                |  0 | det          | True      | the     | DET            | Xxx     | False      | DT    |
                |  1 | amod         | True      | quick   | ADJ            | xxxx    | False      | JJ    |
                |  2 | amod         | True      | brown   | ADJ            | xxxx    | False      | JJ    |
                |  3 | ROOT         | True      | fox     | NOUN           | xxx     | False      | NN    |
                +----+--------------+-----------+---------+----------------+---------+------------+-------+
        :return:
            an analysis DataFrame
        """
        from nlusvc.core.svc import PerformPosAnalysis
        if not self._pos_analyzer:
            self._pos_analyzer = PerformPosAnalysis(is_debug=self.is_debug)
        return self._pos_analyzer.process(some_input)

    def ngrams(self,
               input_text: str,
               gram_level: int,
               noun_phrases_only: bool = True):
        pass

    def remove_stop_words(self,
                          input_text: str,
                          aggressive: bool = False):
        """
        Purpose:
            Remove Stop Words from Input Text
        Sample Input:
            abap for sap_business_one 2 0 exam code
        Sample Output:
            abap sap_business_one exam code
        :param input_text:
            any input text
        :param aggressive:
            True        move beyond traditional stopwords
                        and remove poorly discriminative terms as well
        :return:
            the input text without stop words
        """
        from nlusvc.core.svc import RemoveStopWords

        svc = RemoveStopWords(is_debug=self.is_debug)
        return svc.process(input_text=input_text,
                           aggressive=aggressive)

    @staticmethod
    def cased_terms(input_text: str or list) -> list:
        """
        Extract Cased Terms

        Sample Input:
            One of these services is Amazon Elastic Compute Cloud, which allows users to
            have at their disposal a virtual cluster of computers, available all the time,
            through the Internet

        Sample Output
            Amazon Elastic Compute Cloud

        :param input_text:
            unstructured input of any length (or a list)
        :return:
            a list of cased terms (0..*)
        """
        from nlusvc.core.dmo import CasedTermExtraction
        if type(input_text) == str:
            return CasedTermExtraction.from_str(input_text)
        elif type(input_text) == list:
            return CasedTermExtraction.from_list(input_text)
        raise DataTypeError("\n".join([
            "Invalid Input DataType"]))

    def sentencizer(self,
                    input_text: str) -> list:
        """
        Transform an input text string into a list of sentences
        :param input_text:
            any unstructured input
        :return:
            a list with 0..* entries
            each entry is a sentence
        """
        from nlutext import PerformSentenceSegmentation
        if not self._sentencizer:
            self._sentencizer = PerformSentenceSegmentation(is_debug=self.is_debug)

        return [str(x) for x in self._sentencizer.process(input_text)]

    def normalize(self,
                  input_text: str) -> str:
        if not self._parser:
            self._parser = TextParser(is_debug=self.is_debug,
                                      ontology_name=self._ontology_name)
        d_result = self._parser.process(as_dataframe=False,
                                        original_ups=input_text,
                                        use_profiler=self.is_debug)
        return d_result["ups"]["normalized"]

    def displacy_spans(self,
                       input_text: str,
                       ontology_names: list,
                       mongo_client: BaseMongoClient,
                       xdm_schema: str = 'supply',
                       result_title: Optional[str] = None,
                       use_schema_elements: bool = True) -> list:
        """
        Purpose:
            spaCy is an open-source NLP engine that comes with a built-in entity visualizer (displacy)
            this function does not train a spaCy model, but it will use Cendant to annotate text
            and compute the spans across the text
        Sample Input:
            Experience on key skills like Solution Manager Datastage SAP Pack ABAP Accelerators BAPI Smartforms Adobe
            forms SAP SD and SAP CRM SAP PI EWM Worked on advanced technology components like ALE EDI Workflow and
            LSMW Migration SAP Solution Manager Strong database skills Object Oriented Programming and development
            knowledge Extensive work on Data Dictionary Objects( Tables Structures Domains Data elements
            Views Lock objects) Involved in 2 implementation 1 up gradation and 3 support projects.
        Sample Tags (annotations):
            [   ['abap for sap hana 2.0', 94],
                ['data migration', 64],
                ['data skill', 0],
                ['data structure', 88],
                ['database skill', 65.6],
                ['domain skill', 0],
                ['experience', 33.6],
                ['implement', 33.6],
                ['infosphere datastage', 65.6],
                ['migrate', 33.6],
                ['object oriented', 65.6],
                ['performance appraisal', 100],
                ['programming skill', 65.6],
                ['project', 33.6],
                ['sap pi', 65.6],
                ['sap sd', 65.6],
                ['sap solution manager', 76.3],
                ['skill migration', 16],
                ['support', 33.6],
                ['upgrade', 33.6],
                ['workflow', 33.6]  ]
        Sample Dimensionality Cluster:
            {   'blockchain': [],
                'quantum': [],
                'cloud': [],
                'system administrator': ['data migration'],
                'database': ['data structure', 'database skill', 'infosphere datastage'],
                'data science': [],
                'hard skill': ['abap for sap hana 2.0', 'implement', 'object oriented', 'programming skill'],
                'other': ['migrate'],
                'soft skill': ['experience', 'performance appraisal', 'project', 'workflow'],
                'project management': [],
                'service management': ['sap pi', 'sap sd', 'sap solution manager', 'support', 'upgrade'] }
        Sample Output:
            [   {'start': 0, 'end': 10, 'label': 'SOFT SKILL'},
                {'start': 47, 'end': 56, 'label': 'DATABASE'},
                {'start': 112, 'end': 118, 'label': 'SERVICE MANAGEMENT'},
                {'start': 131, 'end': 137, 'label': 'SERVICE MANAGEMENT'},
                {'start': 196, 'end': 204, 'label': 'SOFT SKILL'},
                {'start': 214, 'end': 223, 'label': 'OTHER'},
                {'start': 224, 'end': 244, 'label': 'SERVICE MANAGEMENT'},
                {'start': 268, 'end': 283, 'label': 'HARD SKILL'},
                {'start': 284, 'end': 295, 'label': 'HARD SKILL'},
                {'start': 439, 'end': 453, 'label': 'HARD SKILL'},
                {'start': 456, 'end': 468, 'label': 'SERVICE MANAGEMENT'},
                {'start': 475, 'end': 482, 'label': 'SERVICE MANAGEMENT'}   ]
        :param input_text:
            any input text
        :param ontology_names:
        :param result_title:
        :param mongo_client:
        :param xdm_schema:
        :param use_schema_elements:
            True        Use Schema Elements instead of Cendant Ontology Tags
                        Sample Output:
                        {   'end': 39,
                            'label': 'DATA SCIENCE',
                            'start': 23,
                            'text': 'Machine Learning',
                            'type': 'tag'}
            False       Use Cendant Ontology Tags instead of Schema Elements
                        Sample Output:
                        {   'end': 39,
                            'label': 'MACHINE LEARNING',
                            'start': 23,
                            'text': 'Machine Learning',
                            'type': 'tag'}
        :return:
        """
        from nlusvc.displacy.svc import GenerateDisplacySpans

        svc = GenerateDisplacySpans(xdm_schema=xdm_schema,
                                    input_text=input_text,
                                    is_debug=self.is_debug,
                                    mongo_client=mongo_client,
                                    ontology_names=ontology_names)

        return svc.process(title=result_title,
                           use_schema_elements=use_schema_elements)

    def preprocess(self,
                   input_text: str,
                   lowercase: bool = True,
                   no_urls: bool = True,
                   no_emails: bool = True,
                   no_phone_numbers: bool = True,
                   no_numbers: bool = True,
                   no_currency_symbols: bool = True,
                   no_punct: bool = True,
                   no_contractions: bool = True,
                   no_accents: bool = True,
                   remove_inside_brackets: bool = False) -> str:
        """

        :param input_text:
        :param lowercase:
        :param no_urls:
        :param no_emails:
        :param no_phone_numbers:
        :param no_numbers:
        :param no_currency_symbols:
        :param no_punct:
        :param no_contractions:
        :param no_accents:
        :param remove_inside_brackets:
        :return:
            preprocessed text
        """
        from nlusvc.core.svc import PreProcessText

        svc = PreProcessText(is_debug=self.is_debug)

        return svc.process(input_text=input_text,
                           lowercase=lowercase,
                           no_urls=no_urls,
                           no_emails=no_emails,
                           no_phone_numbers=no_phone_numbers,
                           no_numbers=no_numbers,
                           no_currency_symbols=no_currency_symbols,
                           no_punct=no_punct,
                           no_contractions=no_contractions,
                           no_accents=no_accents,
                           remove_inside_brackets=remove_inside_brackets)
