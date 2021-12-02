#!/usr/bin/env python
# -*- coding: UTF-8 -*-


from base import BaseObject
from datamongo.collections import CendantTag
from datamongo.core.dmo import BaseMongoClient
from datamongo.text.dmo import TextQueryFilter
from datamongo.text.dmo import TextQueryGenerator
from nlutext import TextParser


class PerformTextQuery(BaseObject):
    """ Perform a Text Query on a Cendant Collection
    """

    _records = None

    def __init__(self,
                 collection_name: str,
                 mongo_client: BaseMongoClient,
                 div_field: list,
                 use_normalized_text: bool = True,
                 key_field: str = None,
                 is_debug: bool = False):
        """
        Created:
            24-Jun-2019
            craig.trim@ibm.com
            *   loosely based on 'query-elasticsearch'
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/371
        Update:
            14-Jul-2019
            anassary@us.ibm.com
            *   add text parser to
        Updated:
            19-August-2019
            abhbasu3@in.ibm.com
            *   added division dictionary {key_field : div_field}
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/515
        Updated:
            22-Aug-2019
            craig.trim@ibm.com
            *   pass text-parser-results rather than text-parser instance
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/796#issuecomment-14041159
        Updated:
            11-Sept-2019
            craig.trim@ibm.com
            *   remove collection-type and collection-category as params and use collection-name instead
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/917
        Updated:
            16-Sept-2019
            craig.trim@ibm.com
            *   pass in the key-field as a param
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/940
        Updated:
            2-Nov-2019
            craig.trim@ibm.com
            *   update 'div-field' param to be a list instead of a str
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1201
            *   remove ability to query-src and rename query-tag to process
        :param collection_name:
            the name of the collection to query
        :param mongo_client:
            the instantiated connection to mongoDB
        :param use_normalized_text:
            True        query on normalized text
            False       query on original (raw) text
        :param div_field:
            None        query on all divisions
            <str>       query on either [GTS, GBS, Cloud]
                        the value is case-insensitive
        :param key_field:
            the key field to restrict the search on
            if None     search all key fields
            Note:       the meaning of the key-field varies by collection
                        supply_tag_*    the "key field" is the Serial Number
                        demand_tag_*    the "key field" is the Open Seat ID
                        learning_tag_*  the "key field" is the Learning Activity ID
        :param is_debug:
            True        write debug statements to the console
        """
        BaseObject.__init__(self, __name__)

        self._is_debug = is_debug
        self._mongo_client = mongo_client
        self._collection_name = collection_name
        self._use_normalized_text = use_normalized_text

        self._text_parser = TextParser()
        self._generator = TextQueryGenerator(div_field=div_field,
                                             key_field=key_field,
                                             is_debug=self._is_debug)

    def _filter(self,
                input_text: str,
                results: list) -> (dict, dict):
        query_filter = TextQueryFilter(input_text=input_text,
                                       results=results,
                                       text_parser_results=self._text_parser.results(),
                                       is_debug=self._is_debug)

        if self._use_normalized_text:
            return query_filter.on_normalized_field()
        return query_filter.on_values_field()

    def execute(self,
                input_text: str):
        cendant_tag = CendantTag(is_debug=self._is_debug,
                                 mongo_client=self._mongo_client,
                                 collection_name=self._collection_name)

        svcresult = self._text_parser.process(input_text)
        normalized_text = svcresult["ups"]["normalized"].strip()

        if self._is_debug:
            self.logger.debug(f"Instantiate Tag Query: "
                              f"Input Text = {normalized_text}")

        query = self._generator.process(normalized_text, case_sensitive=True)
        return cendant_tag.collection.find_by_query(query)

    def process(self,
                input_text: str) -> (dict or None, dict):

        results = self.execute(input_text)

        if not results or not len(results):
            if self._is_debug:
                self.logger.debug(f"No Query Results: "
                                  f"Input Text = {input_text}")
            return None, None

        results, division = self._filter(input_text, results)

        if self._is_debug:
            self.logger.debug(f"Query Results: "
                              f"Input Text = {input_text}, "
                              f"Total={len(results)})")

        return results, division


if __name__ == "__main__":
    ptq = PerformTextQuery(collection_name='supply_tag_20191025',
                           mongo_client=BaseMongoClient(),
                           div_field=['GBS', 'GtS'],
                           is_debug=True)
    ptq.process("help_desk")
