#!/usr/bin/env python
# -*- coding: UTF-8 -*-


from base import BaseObject


class TextQueryGenerator(BaseObject):
    """ Perform a Text Query on a Cendant Collection
    """

    _records = None

    def __init__(self,
                 div_field: list,
                 key_field: str = None,
                 is_debug: bool = False):
        """
        Created:
            2-Nov-2019
            craig.trim@ibm.com
            *   update 'div-field' param to be a list instead of a str
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1201
            *   refactored out of 'text-query-executor'
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

        def _div_field():
            return [x.strip().lower() for x in div_field]

        def _key_field():
            if key_field:
                return key_field.upper()

        self._is_debug = is_debug
        self._div_field = _div_field()
        self._key_field = _key_field()

    def _div_field_query(self) -> dict:

        if len(self._div_field) == 1:
            return {"div_field": self._div_field[0]}

        if len(self._div_field) > 1:
            q = {"$or": []}
            [q["$or"].append({"div_field": x})
             for x in self._div_field]
            return q

        raise NotImplementedError

    def process(self,
                input_text: str,
                case_sensitive: bool) -> dict:
        """
        Purpose:
            Orchestrate the Construction of a Text Query with Parameters
        :param input_text:
            the text to search on
        :param case_sensitive:
            True        case sensitive search
            False       case insensitive search
        :return:
            a text query enhanced with segmentation parameters
        """

        def text_query():
            """
            Purpose:
                Construct the initial Text Query
                the query will be either
                -   case sensitive (efficient)
                -   case insensitive (long-running times)
            :return:
                a basic text query
            """
            if case_sensitive:
                return {"$text": {"$search": '{}'.format(input_text)}}
            return {"$text": {"$search": '/^{}$/i'.format(input_text)}}

        def augment_query(a_query: dict) -> dict:
            # Restrict by Division and Key Field (e.g., Serial Number)
            if self._div_field and self._key_field:
                q = {"$and": [a_query,
                              self._div_field_query(),
                              {"key_field": self._key_field}]}
                return q

            # Restrict by Key Field (e.g., Serial Number)
            if self._key_field:
                return {"$and": [a_query,
                                 {"key_field": self._key_field}]}

            # Restrict by Division
            if len(self._div_field):
                q = {"$and": [a_query,
                              self._div_field_query()]}
                return q

            return a_query

        query = augment_query(text_query())
        if self._is_debug:
            self.logger.debug(f"Generated MongoDB Query: {query}")

        return query


if __name__ == "__main__":
    print(TextQueryGenerator(div_field=[], key_field="asdfa", is_debug=True).process(input_text="here",
                                                                                     case_sensitive=False))
