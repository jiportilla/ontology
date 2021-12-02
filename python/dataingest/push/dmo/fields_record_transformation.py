#!/usr/bin/env python
# -*- coding: UTF-8 -*-


import pandas as pd
from pandas import DataFrame
from tabulate import tabulate

from base import BaseObject


class FieldsRecordTransformation(BaseObject):
    """
    Purpose:
        Transform the non-tag parts of the fields arrays into a flat DataFrame

    Sample Input:
        {   'div_field': '',
            'fields': [
                ...
                {   'agent': 'user',
                    'collection': {'name': 'ingest_cv_education_', 'type': 'ingest_cv_education'},
                    'field_id': 'ff6d19a4-f75b-11e9-91a8-066122a69d41-0',
                    'name': 'major',
                    'normalized': ['electronics and communication'],
                    'tags': {'supervised': [['communication', 96.2]], unsupervised': []},
                    'transformations': [],
                    'type': 'long-text',
                    'value': ['Electronics and Communication']},
                {   'agent': 'user',
                    'collection': {'name': 'ingest_cv_education_', 'type': 'ingest_cv_education'},
                    'field_id': 'ff6d1ad0-f75b-11e9-91a8-066122a69d41-0',
                    'name': 'degree_name',
                    'normalized': ['bachelor_of_engineering'],
                    'tags': {'supervised': [['bachelor of engineering', 99.4]], unsupervised': []},
                    'transformations': [],
                    'type': 'long-text',
                    'value': ['Diploma in Electronics and Communication']},
                {   'agent': 'user',
                    'collection': {'name': 'ingest_cv_education_', 'type': 'ingest_cv_education'},
                    'field_id': 'ff6d1c06-f75b-11e9-91a8-066122a69d41-0',
                    'name': 'thesis_title',
                    'normalized': ['thesis_title bachelor_of_engineering and communicationyear of complete 2005'],
                    'tags': {'supervised': [['bachelor of engineering', 96.3]], unsupervised': []},
                    'transformations': [],
                    'type': 'long-text',
                    'value': ['Thesis title  Diploma in Electronics and CommunicationYear of completion  2005']}],
                    'key_field': '05817Q744'}

    Sample Output:
        +----+---------------------+----------------------------------------+--------------+------------+-----------------------------------------------------------------------------+--------------------------------------------------------------------------------+-------------------------+
        |    | Collection          | FieldId                                | FieldName    | KeyField   | NormalizedText                                                              | OriginalText                                                                   | PriorCollection         |
        |----+---------------------+----------------------------------------+--------------+------------+-----------------------------------------------------------------------------+--------------------------------------------------------------------------------+-------------------------+
        |  0 | supply_tag_20191025 | ff6d19a4-f75b-11e9-91a8-066122a69d41-0 | major        | 05817Q744  | electronics and communication                                               | Electronics and Communication                                                  | ingest_cv_education_ |
        |  1 | supply_tag_20191025 | ff6d1ad0-f75b-11e9-91a8-066122a69d41-0 | degree_name  | 05817Q744  | bachelor_of_engineering                                                     | Diploma in Electronics and Communication                                       | ingest_cv_education_ |
        |  2 | supply_tag_20191025 | ff6d1c06-f75b-11e9-91a8-066122a69d41-0 | thesis_title | 05817Q744  | thesis_title bachelor_of_engineering and communicationyear of complete 2005 | Thesis title  Diploma in Electronics and CommunicationYear of completion  2005 | ingest_cv_education_ |
        +----+---------------------+----------------------------------------+--------------+------------+-----------------------------------------------------------------------------+--------------------------------------------------------------------------------+-------------------------+
    """

    def __init__(self,
                 records: list,
                 collection_name: str,
                 is_debug: bool = False):
        """
        Created:
            23-Feb-20202
            xavier.verges@es.ibm.com
        :param records:
        :param is_debug:
        """
        BaseObject.__init__(self, __name__)

        self._records = records
        self._is_debug = is_debug
        self._collection_name = collection_name

    def process(self,
                unused: float = None) -> DataFrame:
        master = []

        def _normalized_text(field):
            x = field["normalized"]
            if type(x) == list and len(x):
                return x[0]
            return None

        for record in self._records:
            key_field = record["key_field"]

            fields = [x for x in record["fields"] if x["type"] == "long-text" and "tags" in x]
            for field in fields:
                field_name = field["name"]
                field_id = field["field_id"]
                original_text = field["value"][0]
                collection = field["collection"]["name"]
                normalized_text = _normalized_text(field)
                if not normalized_text:
                    continue

                master.append({
                    "KeyField": key_field,
                    "FieldId": field_id,
                    "FieldName": field_name,
                    "OriginalText": original_text,
                    "NormalizedText": normalized_text,
                    "Collection": self._collection_name,
                    "PriorCollection": collection})

        df = pd.DataFrame(master)

        if self._is_debug and not df.empty:
            self.logger.debug("\n".join([
                f"TAG Transformation Complete (collection={self._collection_name}, total={len(df)})",
                tabulate(df.sample(),
                         tablefmt='psql',
                         headers='keys')]))

        return df
