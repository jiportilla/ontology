#!/usr/bin/env python
# -*- coding: UTF-8 -*-


import pandas as pd
from pandas import DataFrame

from base import BaseObject
from base import MandatoryParamError
from datadict import FindEntity
from nlutext import TextParser
from tabulate import tabulate


class GeneratePhraseTags(BaseObject):

    def __init__(self,
                 some_text: str,
                 ontology_name:str='base',
                 is_debug: bool = False):
        """
        Created:
            9-May-2019
            craig.trim@ibm.com
            *   contain functionality for api call
        Updated:
            29-August-2019
            abhbasu3@in.ibm.com
            *   added `confidence` column to the dataframe
            *   https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/853
        Updated:
            13-Dec-2019
            craig.trim@ibm.com
            *   load dictionaries by ontology name
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1582
        """
        BaseObject.__init__(self, __name__)
        if not some_text:
            raise MandatoryParamError("Text")

        self.text = some_text
        self.is_debug = is_debug

        self.text_parser = TextParser(is_debug=self.is_debug,
                                      ontology_name=ontology_name)
        self.entity_finder = FindEntity(is_debug=self.is_debug,
                                        ontology_name=ontology_name)

    def process(self) -> DataFrame:
        d_result = self.text_parser.process(original_ups=self.text,
                                            use_profiler=self.is_debug)

        df_results = pd.DataFrame()
        tags = []
        confidence = []

        def _update(a_tag: str) -> None:
            tags.append(a_tag[0])
            confidence.append(a_tag[1])

        [_update(x) for x in d_result["tags"]["supervised"]]
        [_update(x) for x in d_result["tags"]["unsupervised"]]

        tags = [self.entity_finder.label_or_self(x) for x in tags]

        df_results["Tag"] = tags
        df_results["Confidence"] = confidence

        if self.is_debug:
            self.logger.debug('\n'.join([
                "Phrase Tag Generation Completed",
                tabulate(df_results,
                         headers='keys',
                         tablefmt='psql')]))

        return df_results
