#!/usr/bin/env python
# -*- coding: UTF-8 -*-


import pandas as pd
from pandas import DataFrame

from base import BaseObject


class ExplainAcronymExpansions(BaseObject):

    def __init__(self,
                 expansions: DataFrame,
                 is_debug: bool = False):
        """
        Created:
            27-Jun-2019
            craig.trim@ibm.com
            *   provide explainations of an existing expansions result-set
                +----+-----------------------------------------+----------+-----------------------------+
                |    | Key                                     | Type     | Value                       |
                |----+-----------------------------------------+----------+-----------------------------|
                |  0 | TF                                      | Acronym  | Tensorflow                  |
                |  1 | NLP                                     | Acronym  | Natural Language Processing |
                |  2 | ML                                      | Acronym  | Machine Learning            |
                |  3 | BPM                                     | Acronym  | Business Process Modeling   |
                |  4 | Deep Learning                           | Instance | RNNs , CNNs                 |
                |  5 | Enterprise Architecture and Development | Instance | Java , Python               |
                |  6 | IBM Master Inventor                     | Instance | 85th Plateau                |
                +----+-----------------------------------------+----------+-----------------------------+

            Sample Output:
            +----+---------------------------------+-----------------------------------------+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+-----------------------------+-----------------------------------------------------------+--------------------------------------------------------------------+----------+
            |    | Cendant                         | Key                                     | Summary                                                                                                                                                                                                                                                                                                          | Value                       | dbPedia                                                   | explanation                                                        | type     |
            |----+---------------------------------+-----------------------------------------+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+-----------------------------+-----------------------------------------------------------+--------------------------------------------------------------------+----------|
            |  0 | ['Tensorflow']                  | TF                                      | TensorFlow is a free and open-source software library for dataflow and differentiable programming across a range of tasks.                                                                                                                                                                                       | Tensorflow                  | https://en.wikipedia.org/wiki/TensorFlow                  | TF is an acronym of Tensorflow                                     | Acronym  |
            |  1 | ['Natural Language Processing'] | NLP                                     | Natural language processing (NLP) is a subfield of computer science, information engineering, and artificial intelligence concerned with the interactions between computers and human (natural) languages, in particular how to program computers to process and analyze large amounts of natural language data. | Natural Language Processing | https://en.wikipedia.org/wiki/Natural_language_processing | NLP is an acronym of Natural Language Processing                   | Acronym  |
            |  2 | ['Machine Learning']            | ML                                      | Machine learning (ML) is the scientific study of algorithms and statistical models that computer systems use in order to perform a specific task effectively without using explicit instructions, relying on patterns and inference instead.                                                                     | Machine Learning            | https://en.wikipedia.org/wiki/Machine_learning            | ML is an acronym of Machine Learning                               | Acronym  |
            |  3 | ['Business Process', 'Model']   | BPM                                     | Business process modeling (BPM) in business process management and systems engineering is the activity of representing processes of an enterprise, so that the current process may be analysed, improved, and automated.                                                                                         | Business Process Modeling   | https://en.wikipedia.org/wiki/Business_process_modeling   | BPM is an acronym of Business Process Modeling                     | Acronym  |
            |  4 | ['Java', 'Python']              | Enterprise Architecture and Development | Enterprise architecture (EA) is "a well-defined practice for conducting enterprise analysis, design, planning, and implementation, using a comprehensive approach at all times, for the successful development and execution of strategy.                                                                        | Java , Python               | https://en.wikipedia.org/wiki/Enterprise_architecture     | Java , Python is a type of Enterprise Architecture and Development | Instance |
            |  5 | ['Java', 'Python']              | Enterprise Architecture and Development | Enterprise architecture (EA) is "a well-defined practice for conducting enterprise analysis, design, planning, and implementation, using a comprehensive approach at all times, for the successful development and execution of strategy.                                                                        | Java , Python               | https://en.wikipedia.org/wiki/Enterprise_architecture     | Java , Python is a type of Enterprise Architecture and Development | Instance |
            |  6 | []                              | IBM Master Inventor                     | An IBM Master Inventor is an individual selected by IBM.                                                                                                                                                                                                                                                         | 85th Plateau                | https://en.wikipedia.org/wiki/IBM_Master_Inventor         | 85th Plateau is a type of IBM Master Inventor                      | Instance |
            |  7 | []                              | IBM Master Inventor                     | An IBM Master Inventor is an individual selected by IBM.                                                                                                                                                                                                                                                         | 85th Plateau                | https://en.wikipedia.org/wiki/IBM_Master_Inventor         | 85th Plateau is a type of IBM Master Inventor                      | Instance |
            |  8 | ['Artificial Neural Network']   | Deep Learning                           | Deep learning (also known as deep structured learning or hierarchical learning) is part of a broader family of machine learning methods based on artificial neural networks.                                                                                                                                     | RNNs , CNNs                 | https://en.wikipedia.org/wiki/Deep_learning               | RNNs , CNNs is a type of Deep Learning                             | Instance |
            +----+---------------------------------+-----------------------------------------+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+-----------------------------+-----------------------------------------------------------+--------------------------------------------------------------------+----------+

        """
        BaseObject.__init__(self, __name__)
        from nlutext import TextParser
        from cendalytics import DBpediaEntityLookup
        from nlutext import PerformSentenceSegmentation

        self.expansions = expansions
        self.text_parser = TextParser(is_debug=is_debug)
        self.dbpedia_lookup = DBpediaEntityLookup(is_debug=is_debug)
        self.sentencizer = PerformSentenceSegmentation(is_debug=is_debug)

    def _generic_explain(self,
                         a_key: str,
                         a_value: str,
                         wiki_lookup: str) -> dict:
        def cendant_annotator() -> list:
            return self.text_parser.process(a_value)["tags"]["supervised"]

        page = self.dbpedia_lookup.find_page(wiki_lookup)

        def dbpedia_url() -> str:
            if page:
                try:
                    return page.url
                except Exception as e:
                    self.logger.error(e)

        def dbpedia_summary() -> str:

            if page:
                try:
                    summary = str(page.summary)
                    sentences = self.sentencizer.process(some_input_text=summary)
                    # self.logger.debug("HERE IS SENTENCES >>>> {}".format(sentences))

                    if sentences and len(sentences):
                        # self.logger.debug("First Sentence Type {}".format(type(sentences[0])))
                        # self.logger.debug("First Sentence {}".format(sentences[0]))
                        return str(sentences[0])

                except Exception as e:
                    self.logger.error(e)

        return {
            "Key": a_key,
            "Value": a_value,
            "Cendant": cendant_annotator(),
            "dbPedia": dbpedia_url(),
            "Summary": dbpedia_summary()}

    def _explain_acronym(self,
                         an_acronym: str,
                         a_value: str) -> dict:

        def explanation():
            return "{} is an acronym of {}".format(an_acronym, a_value)

        svcresult = self._generic_explain(a_key=an_acronym,
                                          a_value=a_value,
                                          wiki_lookup=a_value)
        svcresult["Type"] = "Acronym"
        svcresult["Explanation"] = explanation()

        return svcresult

    def _explain_instance(self,
                          an_instance: str,
                          a_value: str) -> dict:

        def explanation():
            return "{} is a type of {}".format(a_value, an_instance)

        svcresult = self._generic_explain(a_key=an_instance,
                                          a_value=a_value,
                                          wiki_lookup=an_instance)
        svcresult["Type"] = "Instance"
        svcresult["Explanation"] = explanation()

        return svcresult

    def process(self) -> DataFrame:

        results = []

        df_acronyms = self.expansions[self.expansions.Type == 'Acronym']
        for _, row in df_acronyms.iterrows():
            results.append(self._explain_acronym(an_acronym=row["Key"],
                                                 a_value=row["Value"]))

        df_instances = self.expansions[self.expansions.Type == 'Instance']
        for _, row in df_instances.iterrows():
            results.append(self._explain_instance(an_instance=row["Key"],
                                                  a_value=row["Value"]))

        return pd.DataFrame(results)
