# !/usr/bin/env python
# -*- coding: UTF-8 -*-


import pandas as pd
from pandas import DataFrame

from base import BaseObject
from datamongo import BaseMongoClient
from datamongo import CendantCollection


class PerformTagSearch(BaseObject):
    """
    Purpose:
    Service that Performs a Search against Tags (annotations) in MongoDB

    Traceability:
    https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1228

    Sample Output:
        +-----+--------------+-----------------------------------------+------------+-----------+-------------------------------------------------+
        |     |   Confidence | FieldId                                 | KeyField   | Tag       | Value                                           |
        |-----+--------------+-----------------------------------------+------------+-----------+-------------------------------------------------|
        |   0 |          0   | b0e75b9a-f75c-11e9-924d-066122a69d41-0  | 078431706  | help desk | BUSINESS EXPERIENCE             Software progra |
        |   1 |          0   | d2049d5a-f75d-11e9-907f-066122a69d41-1  | 002013822  | help desk | LTU9607F 0 Minutes 11 Mar 06  Elements of Proje |
        |   2 |         83   | bdfad4e2-f75c-11e9-924d-066122a69d41-7  | 380489616  | help desk | Provide second level technical support for comp |
        |   3 |         40   | bdfad4e2-f75c-11e9-924d-066122a69d41-8  | 380489616  | help desk | ensure Business and Support SLAs are maintained |
        ...
        | 428 |          0   | f4d1791c-f75c-11e9-a58b-066122a69d41-0  | 4D8177897  | help desk | Started work as part time paraprofessional in a |
        +-----+--------------+-----------------------------------------+------------+-----------+-------------------------------------------------+

    Prereq:
    an index on `fields.tags.supervised` should exist for performance reasons
    """

    __cache = {}

    def __init__(self,
                 tags: list,
                 collection_name: str,
                 div_field: list or None,
                 key_field: str or None,
                 server_alias: str = 'cloud',
                 is_debug: bool = False):
        """
        Created:
            31-Oct-2019
            craig.trim@ibm.com
        Updated:
            2-Nov-2019
            craig.trim@ibm.com
            *   update 'div-field' param to be a list instead of a str
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1201
        Updated:
            3-Nov-2019
            craig.trim@ibm.com
            *   add the 'transform-type' method
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1234#issuecomment-15707274
        Updated:
            13-Nov-2019
            craig.trim@ibm.com
            *   enforce list as a return type for records
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1349#issuecomment-15988093
        Updated:
            03-Dec-2019
            abhbasu3@in.ibm.com
            *   Fetch start date, end date, region for SkillsReportAPI
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1473
        Updated:
            13-Feb-2020
            craig.trim@ibm.com
            *   Pass in Server Alias as a parameter
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1855
        :param tags:
            the tags to search for
        :param collection_name:
            the target collection to query
        :param div_field:
            the list of divisions (e.g., 'GTS' or 'GBS') to restrict searches for
            if None     search all divisions
            Note:
                Use the API method 'division-distribution' to find existing divisions
        :param key_field:
            the key field to restrict the search on
            if None     search all key fields
            Note:       the meaning of the key-field varies by collection
                        supply_tag_*    the "key field" is the Serial Number
                        demand_tag_*    the "key field" is the Open Seat ID
                        learning_tag_*  the "key field" is the Learning Activity ID
        :param is_debug:
            if True     increase log output at DEBUG level
        """
        BaseObject.__init__(self, __name__)

        self._tags = tags
        self._is_debug = is_debug
        self._div_field = div_field
        self._key_field = key_field

        def mongo_client() -> BaseMongoClient:
            if server_alias.lower() == 'cloud':
                return BaseMongoClient(is_debug=is_debug,
                                       server_alias=server_alias)
            return BaseMongoClient(is_debug=is_debug,
                                   server_alias='wftag')

        self._collection = CendantCollection(some_base_client=mongo_client(),
                                             some_collection_name=collection_name,
                                             is_debug=is_debug)

    @staticmethod
    def _matching_fields(search_tag: str,
                         a_record: dict) -> list:
        matching_fields = []
        region = []
        start_date = []
        end_date = []
        region_name = None
        startdate = None
        enddate = None
        for field in a_record["fields"]:
            if "name" in field:
                if field["name"] == "region":
                    region_name = field["value"][0]
                if field["name"] == "start_date":
                    startdate = field["value"][0]
                if field["name"] == "end_date":
                    enddate = field["value"][0]
            if "tags" in field:
                if "supervised" in field["tags"]:
                    field_tags = [x[0].lower().strip()
                                  for x in field["tags"]["supervised"]]
                    if search_tag.lower() in field_tags:
                        matching_fields.append(field)
                        region.append(region_name)
                        start_date.append(startdate)
                        end_date.append(enddate)

        return matching_fields, region, start_date, end_date

    @staticmethod
    def _matching_tag(search_tag: str,
                      a_matching_field: dict) -> tuple:
        for tag in a_matching_field["tags"]["supervised"]:
            if tag[0].lower() == search_tag.lower():
                return tag
        raise ValueError

    def _init_query(self):
        q = {}
        if len(self._div_field):
            q["$or"] = []
            for division in self._div_field:
                q["$or"].append({"div_field": division.lower()})
            return q
        if self._key_field:
            q = {"key_field": self._key_field.upper()}
        return q

    @staticmethod
    def _transform_type(a_result: dict) -> str:
        """
        Purpose:
            Remove 'ingest_' prefix
        Reference:
            https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1234#issuecomment-15707274
        Sample Input:
            ingest_career_history_
        Sample Output:
            career_history_
        :return:
            a transformed string
        """
        value = a_result["collection"]["name"]
        if value.lower().startswith("ingest_"):
            return value[7:].strip()
        return value

    def _records(self) -> list:
        key = ''.join(self._div_field)
        if key in self.__cache:
            if self._is_debug:
                self.logger.debug(f"Cached Record Retrieval ("
                                  f"key={key}, "
                                  f"keys={sorted(self.__cache.keys())})")
            return self.__cache[key]

        def query() -> list:
            if self._key_field:
                return [self._collection.by_key_field(self._key_field)]
            if self._div_field:
                return self._collection.by_field("div_field", self._div_field)
            return self._collection.all()

        results = query()
        if type(results) == dict:  # GIT-1349-15988093
            results = [results]

        self.__cache[key] = results
        return self.__cache[key]

    def process(self) -> DataFrame or None:

        master = []
        records = self._records()

        if not records or not len(records):
            self.logger.warning('\n'.join([
                f"No Records Located",
                f"\tDivision: {self._div_field}",
                f"\tKey Field: {self._key_field}"]))
            return None

        if self._is_debug:
            self.logger.debug('\n'.join([
                f"Located Records (total={len(records)})",
                f"\tDivision: {self._div_field}",
                f"\tKey Field: {self._key_field}"]))

        for record in records:

            for tag in self._tags:

                matching_fields, region, start_date, end_date = self._matching_fields(search_tag=tag,
                                                                                      a_record=record)
                if not len(matching_fields):
                    continue

                for matching_field in matching_fields:
                    tag_match = self._matching_tag(tag, matching_field)

                    master.append({
                        "KeyField": record["key_field"],
                        "Division": record["div_field"].upper(),
                        "FieldId": matching_field["field_id"],
                        "Region": region[0],
                        "start_date": start_date[0],
                        "end_date": end_date[0],
                        "Value": matching_field["value"][0],
                        "Type": self._transform_type(matching_field),
                        "Normalized": matching_field["normalized"][0],
                        "Tag": tag_match[0],
                        "Confidence": tag_match[1]})

        if self._is_debug:
            self.logger.debug('\n'.join([
                f"Filtered Records (total={len(master)})",
                f"\tTags: {self._tags}",
                f"\tDivision: {self._div_field}",
                f"\tKey Field: {self._key_field}"]))

        return pd.DataFrame(master)

    def suppl_tag_to_df(self, persons: list) -> DataFrame:
        records = []
        for person in persons:
            fields = [field for field in person['fields'] if 'tags' in field]
            for field in fields:
                for tag in self._tags:
                    try:
                        stat = [t[1] for t in field['tags']['supervised'] if t[0] == tag]
                        confidence = float(stat[0])
                    except:
                        continue

                    typ = field['collection']['name']
                    if typ.lower().startswith("ingest_"):
                        typ = typ[7:].strip()
                    records.append({'KeyField': person['key_field'],
                                    'Division': person['div_field'].upper(),
                                    'FieldId': field['field_id'],
                                    'Value': field['value'][0],
                                    'Type': typ,
                                    'Normalized': str(field['normalized']).strip('[]\''),
                                    'Tag': tag,
                                    'Confidence': confidence})

        return (DataFrame(records))

    def tag_search(self, logic_operator: str = 'and') -> DataFrame:
        ################################################################
        #                  variants of logical operators               #
        #     1 - and       2 - not          3- nor          4-or      #
        #                      full documentation on                   #
        #   docs.mongodb.com/manual/reference/operator/query-logical/  #
        ################################################################
        try:
            operator = '$' + logic_operator
            query = {operator: []}
            for tag in self._tags:
                query[operator].append(
                    {"fields":
                         {"$elemMatch":
                              {"tags.supervised":
                                   {"$elemMatch": {"$elemMatch": {"$eq": str(tag)}}}
                               }
                          }
                     }
                )

            if self._div_field is not None:
                query = {'$and': [query, {'div_field': self._div_field}]}
            result = self._collection.find_by_query(query)
            return (self.suppl_tag_to_df(result))

        except NameError as error:
            print('ERROR:  ' + str(error))
