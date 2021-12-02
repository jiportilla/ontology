#!/usr/bin/env python
# -*- coding: UTF-8 -*-


from base import BaseObject


class TextQueryFilter(BaseObject):
    """ Filter Text Query Results
    """

    _records = None

    def __init__(self,
                 results: list,
                 input_text: str,
                 text_parser_results: dict,
                 is_debug: bool = False):
        """
        Created:
            24-Jun-2019
            craig.trim@ibm.com
            *   loosely based on 'query-elasticsearch'
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/371
        Updated:
            14-Jul-2019
            anassar@us.ibm.com
            *   add the text parser in order to use the normalized text
                within the on normalize filter method
        Updated:
            19-August-2019
            abhbasu3@in.ibm.com
            *   fetch div_field from datasource  and create dictionary {key_field : div_field}
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/515
        Updated:
            21-August-2019
            abhbasu3@in.ibm.com
            *   fetch collection type as evidence `type` from corresponding field and create a evidence `type`->`value` mapping
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/764
        Updated:
            22-Aug-2019
            craig.trim@ibm.com
            *   pass text-parser-results rather than text-parser instance
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/796#issuecomment-14041159
        Updated:
            17-Sept-2019
            craig.trim@ibm.com
            *   defect fix when filtering by original text
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/941#issuecomment-14691234
        Updated:
            16-Oct-2019
            craig.trim@ibm.com
            *   update data structure traversal to support supply-src collections
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1122#issuecomment-15338556
        :param input_text:
            the query string
        :param text_parser_results
            the text parser results
        :param results:
            the query results
        :param is_debug:
        """
        BaseObject.__init__(self, __name__)

        self.results = results
        self.input_text = input_text.lower().strip()
        self._text_parser_results = text_parser_results

        self.is_debug = is_debug

    @staticmethod
    def _is_valid(query_term: str,
                  text: str) -> bool:

        text = text.lower().strip()
        query_term = query_term.lower().strip()

        if query_term == text:
            return True

        fq = " {} ".format(query_term)
        if fq in text:
            return True

        lq = " {}".format(query_term)
        if text.endswith(lq):
            return True

        rq = "{} ".format(query_term)
        if text.startswith(rq):
            return True

        return False

    def _get_normalized_text(self):
        text_filter = self.input_text
        if self._text_parser_results:
            if "ups" in self._text_parser_results:
                if "normalized" in self._text_parser_results["ups"]:
                    text_filter = self._text_parser_results["ups"]["normalized"]
        return text_filter

    def on_normalized_field(self) -> (dict, dict):
        """
        Return only the field data that matches, rather than the entire JSON record and its corresponding division
        :return:
            filtered results
            division
        """

        text_filter = self._get_normalized_text()
        d_normalized = {}
        division = {}

        for result in self.results:
            division[result["key_field"]] = result["div_field"]

            normalized = []

            fields = [field for field in result["fields"]
                      if "normalized" in field]

            for field in fields:

                # 941#issuecomment-14691234
                values = [value for value in field["normalized"]
                          if type(value) == str]

                for value in values:
                    if self._is_valid(text_filter, value):
                        # get data type
                        if "ingest_" in field["collection"]:
                            data_type = field["collection"].replace("ingest_", "")
                        else:
                            data_type = field["collection"]

                        # filter data for type & value
                        normalized.append({
                            "type": data_type,
                            "value": value})

            if len(normalized):
                d_normalized[result["key_field"]] = sorted(normalized, key=lambda x: x['value'])

        return d_normalized, division

    def on_values_field(self) -> (dict, dict):
        """
        Return only the field data that matches, rather than the entire JSON record and its corresponding division
        :return:
            filtered results
            division
        """

        d_normalized = {}
        division = {}

        for result in self.results:
            division[result["key_field"]] = result["div_field"]

            normalized = []

            fields = [field for field in result["fields"]
                      if "value" in field]

            # Match the source fields in SRC collections
            for field in fields:

                def values():
                    if type(field["value"]) == list:  # 1122#issuecomment-15338556
                        return [x for x in field["value"]
                                if type(x) == str]  # 941#issuecomment-14691234
                    return [str(field["value"])]

                for value in values():
                    if self._is_valid(self.input_text, value):
                        # get data type
                        if "ingest_" in field["collection"]:
                            data_type = field["collection"].replace("ingest_", "")
                        else:
                            data_type = field["collection"]

                        # filter data for type & value
                        normalized.append({
                            "type": data_type,
                            "value": value})

            if len(normalized):
                d_normalized[result["key_field"]] = sorted(normalized, key=lambda x: x['value'])

        return d_normalized, division
