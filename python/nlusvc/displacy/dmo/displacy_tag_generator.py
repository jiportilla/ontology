#!/usr/bin/env python
# -*- coding: UTF-8 -*-


from typing import Optional
from typing import Tuple

from base import BaseObject
from base import DataTypeError
from nlutext import MultiTextParser


class DisplacyTagGenerator(BaseObject):
    """ Annotate the Input Text

    Sample Input:
        +----+--------------+------------------------+-----------------------+------------+----------------+
        |    |   Confidence | InputText              | NormalizedText        | Ontology   | Tag            |
        |----+--------------+------------------------+-----------------------+------------+----------------|
        |  0 |         97.3 | exocrine gland red hat | exocrine_gland redhat | base       | redhat         |
        |  1 |         98.3 | exocrine gland red hat | exocrine_gland redhat | biotech    | exocrine gland |
        +----+--------------+------------------------+-----------------------+------------+----------------+

    Sample Output:
        [   'redhat', 'exocrine gland'   ]

    :return:
        a list of tag tuples
            [   (redhat, 97.3, base),
                (exocrine gland, 98.3, biotech) ]
    """

    def __init__(self,
                 input_text: str,
                 ontology_names: list,
                 is_debug: bool = False):
        """
        Created:
            14-Dec-2019
            craig.trim@ibm.com
            *   refactored out of 'generate-display-spans' in pursuit of
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1594
        :param input_text:
        :param is_debug:
        """
        BaseObject.__init__(self, __name__)

        if type(ontology_names) != list:
            raise DataTypeError("Ontology Names, list")

        self._is_debug = is_debug
        self._input_text = input_text
        self._ontology_names = ontology_names

    def process(self) -> Tuple[list, str]:
        parser = MultiTextParser(ontology_names=self._ontology_names,
                                 is_debug=self._is_debug)

        a_df = parser.process(original_ups=self._input_text,
                              use_profiler=self._is_debug,
                              as_dataframe=True)

        tags = []
        for _, row in a_df.iterrows():
            tags.append((row["Tag"], row["Confidence"], row["Ontology"]))

        def _normalized_text() -> Optional[str]:
            if not a_df.empty:
                return str(a_df.iloc[0]['NormalizedText'])

        return tags, _normalized_text()
