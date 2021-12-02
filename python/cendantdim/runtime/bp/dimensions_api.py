#!/usr/bin/env python
# -*- coding: UTF-8 -*-


from graphviz import Digraph
from networkx import Graph
from pandas import DataFrame

from base import BaseObject
from datamongo import BaseMongoClient
from datamongo import CendantXdm
from datamongo import CollectionFinder


class DimensionsAPI(BaseObject):

    def __init__(self,
                 mongo_client: BaseMongoClient = None,
                 is_debug: bool = False):
        """
        Created:
            29-Apr-2019
            craig.trim@ibm.com
            *   created to expose dimensionality APIs via Jupe
        Updated:
            2-May-2019
            craig.trim@ibm.com
            *   better caching of collections
        Updated:
            13-May-2019
            craig.trim@ibm.com
            *   large ongoing refactoring - moving methods into services
        Updated:
            21-May-2019
            craig.trim@ibm.com
            *   add 'region' to 'by-slot-value'
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/243
        Updated:
            25-Jul-2019
            craig.trim@ibm.com
            *   added 'test-random-record'
                https://github.ibm.com/-cdo/unstructured-analytics/issues/480
        Updated:
            7-Aug-2019
            craig.trim@ibm.com
            *   remove use of '-dimensions' in favor of 'cendant-xdm'
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/649
        Updated:
            26-Sept-2019
            craig.trim@ibm.com
            *   added 'mean' method
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1010
        Updated:
            27-Sept-2019
            *   added mongo collection name as input parameter to evidence, by_slot_value_for_demand
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1004
        Updated:
            7-Oct-2019
            craig.trim@ibm.com
            *   renamed 'by-slot-value' to 'by-slot-value-for-supply' (consistency fix)
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1048
        Updated:
            15-Oct-2019
            craig.trim@ibm.com
            *   update parameter list for 'by-key-fields'
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1116#issuecomment-15311064
        Updated:
            4-Nov-2019
            craig.trim@ibm.com
            *   cleaned up API
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1265
        """
        BaseObject.__init__(self, __name__)
        self._is_debug = is_debug
        if not mongo_client:
            mongo_client = BaseMongoClient()

        self.mongo_client = mongo_client

    def test_schema_elements(self,
                             xdm_schema: str,
                             collection_name_tag: str,
                             host_name: str = None,  # deprecated/ignored
                             database_name: str = 'cendant',
                             skip: int = None,
                             limit: int = None) -> DataFrame:
        """
        Purpose:
            For a given collection, generate the Schemas and Tags with their relative frequencies by order of appearance
        Rationale:
            Useful for testing to continually hone in on 'unlisted' and  'other' categories
        Reference:
            https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1205#issuecomment-15589827
        Sample Output:
            +------+---------+-----------------------------+-------------------------------------------------------------------+
            |      |   Count | Schema                      | Tag                                                               |
            |------+---------+-----------------------------+-------------------------------------------------------------------|
            |    0 |    8496 | unlisted                    | ibm                                                               |
            |    1 |    5469 | unlisted                    | problem solving                                                   |
            |    2 |    4641 | unlisted                    | project                                                           |
            |    3 |    4399 | service management          | support                                                           |
            |    4 |    4361 | unlisted                    | implement                                                         |
            |    5 |    3827 | project management          | management                                                        |
            ...
            | 2158 |       1 | database                    | db2 bind                                                          |
            +------+---------+-----------------------------+-------------------------------------------------------------------+
        :param xdm_schema:
            the schema to use in the search
            e.g., 'supply' or 'learning'
        :param collection_name_tag:
            a tag collection name
        :param host_name:
            deprecated/ignored
        :param database_name:
            e.g., cendant
        :param skip:
        :param limit:
        :return:
            a DataFrame of Schemas, Tags and their associated frequencies
        """
        from cendantdim.runtime.svc import SchemaElementsFrequency

        return SchemaElementsFrequency(xdm_schema=xdm_schema,
                                       collection_name_tag=collection_name_tag,
                                       mongo_client=BaseMongoClient(),
                                       database_name=database_name,
                                       is_debug=self._is_debug).process(skip=skip, limit=limit)

    def cluster(self,
                tags: list,
                ontology_name: str,
                xdm_schema: str) -> dict:
        """
        Purpose:
            Given a list of tags, return the tags in a dimensionality cluster
        Sample Input:
            ['private cloud', 'python', 'mapreduce', 'leadership']
        Sample Output:
            {   'data science': ['python', 'mapreduce'],
                'cloud':        ['private cloud'],
                'soft skill':   ['leadership'],
                'hard skill':   [],
                'learning':     [],
                'other':        [],
                'project management': [],
                'service management': []    }
        :param tags:
            a list of tags to be organized into a dimensionality schema
        :param ontology_name:
            the ontology to use in the search
            e.g., 'base' or 'biotech'
        :param xdm_schema:
            the schema to use in the search
            e.g., 'supply' or 'learning'
        :return:
            a dictionary that represents the clustered tags
        """
        from cendantdim.runtime.svc import ClusterTagsIntoDimensions

        return ClusterTagsIntoDimensions(tags=tags,
                                         xdm_schema=xdm_schema,
                                         ontology_name=ontology_name,
                                         is_debug=self._is_debug).process()

    @staticmethod
    def schema(schema_name: str) -> DataFrame:
        """
        Purpose:
            Visualize a Schema in NetworkX
        :param schema_name:
            any schema (e.g., 'Learning', 'Dimensions', etc)
        :return:
            a NetworkX Graph that is neither rendered nor persisted
        """
        from cendantdim.runtime.svc import GenerateSchemaNetworkX

        return GenerateSchemaNetworkX.by_schema(schema_name)

    @staticmethod
    def networkx(some_input) -> Graph:
        """
        :param some_input:
            either a dataframe or series generated via the `inference` method
        :return:
            a NetworkX Graph that is neither rendered nor persisted
        """
        from cendantdim.runtime.svc import GenerateSchemaNetworkX

        return GenerateSchemaNetworkX(some_input).process()

    def graphviz(self,
                 some_input) -> Digraph:
        """
        :param some_input:
             dataframe generated contains parent -> child relation
        :return:
            a graphviz directed Graph that is neither rendered nor persisted
        """
        from cendantdim.runtime.svc import GenerateSchemaGraphviz

        return GenerateSchemaGraphviz(some_input,
                                      is_debug=self._is_debug).process()

    def starplot(self,
                 xdm_schema: str,
                 df_record: DataFrame,
                 divisor: int = 10,
                 minimum: int = 3) -> DataFrame:
        """
        Purpose:
            Generate a Pandas DataFrame as input to a Seaborn Starplot
        :param xdm_schema:
            the schema to use in the search
            e.g., 'supply' or 'learning'
        :param df_record:
            the input dataframe
        :param divisor:
            this decreases values evenly (starplot aethetic technique)
        :param minimum:
            the minimum value (starplot aethetic technique)
        :return:
            a dataframe that can be used as seaborn input
        """
        from cendantdim.runtime.svc import GenerateDataFrameStarPlot

        generator = GenerateDataFrameStarPlot(xdm_schema=xdm_schema,
                                              df_record=df_record,
                                              mongo_client=self.mongo_client,
                                              is_debug=self._is_debug)

        return generator.process(divisor=divisor,
                                 minimum=minimum)

    def evidence(self,
                 collection_name: str,
                 key_field: str) -> dict:
        """
        Get evidence by key_field
        :param collection_name:
            mongo collection name
        :param key_field:
            openseat id value
        :return:
            the requested record
        """
        collection = CendantXdm(collection_name=collection_name,
                                mongo_client=self.mongo_client,
                                is_debug=self._is_debug)

        svcresult = collection.collection.by_key_field(key_field)  # GIT-1415-16099357

        return svcresult

    def frequency_summary(self) -> DataFrame:
        from cendantdim.runtime.svc import FindFrequencySummary
        return FindFrequencySummary(self.mongo_client,
                                    is_debug=self._is_debug).process()

    def frequency(self,
                  source_name: str,
                  limit: int = None) -> DataFrame:
        from cendantdim.runtime.svc import FindDimensionFrequency

        cendant_xdm = CendantXdm(is_debug=self._is_debug,
                                 mongo_client=self.mongo_client,
                                 collection_name=CollectionFinder.find_xdm(source_name))

        return FindDimensionFrequency(cendant_xdm).process(limit=limit)

    def find_matching_supply(self,
                             open_seat_id: str,
                             focal_slots: list,
                             match_limit: int = None,
                             minimum_band_level: int = None,
                             maximum_band_level: int = None):
        """
        Purpose:
            find matching supply reords
        :param open_seat_id:
            the open seat record (demand)
        :param focal_slots:
             the slots to focus the match on (e.g. ['data science', 'hard skill'])
        :param match_limit:
            the total records to return
            None        return all records
        :param minimum_band_level:
            the minimum band level to search by
            None        no lower limit
        :param maximum_band_level:
            the maximum band level to search by
            None        no upper limit
        :return:
            the matching supply records
        """
        pass

    def by_value_sum(self,
                     source_name: str,
                     minimum_value_sum: int = None,
                     maximum_value_sum: int = None,
                     key_fields_only: bool = False) -> list:
        from cendantdim.runtime.svc import FindDimensionRecordsBySum
        finder = FindDimensionRecordsBySum(mongo_client=self.mongo_client,
                                           is_debug=self._is_debug)
        return finder.process(source_name=source_name,
                              minimum_value_sum=minimum_value_sum,
                              maximum_value_sum=maximum_value_sum,
                              key_fields_only=key_fields_only)

    def region_histogram(self,
                         source_name) -> DataFrame:
        """
        Purpose:
            Create a DataFrame that shows a Histogram of Regions by Dimension
        :param source_name:
            the source collection to search in
                e.g.,  Supply, Demand, Learning (case insensitive)
        :return:
            a DataFrame histogram
        """
        from cendantdim.runtime.svc import FindDimensionRecordsByRegion
        finder = FindDimensionRecordsByRegion(source_name=source_name,
                                              mongo_client=self.mongo_client,
                                              is_debug=self._is_debug)
        return finder.process()

    def by_slot_value_for_demand(self,
                                 collection_name: str,
                                 slot_name: str,
                                 status: str = None,
                                 region: str = None,
                                 start_date: str = None,
                                 end_date: str = None,
                                 minimum_value_sum: float = None,
                                 maximum_value_sum: float = None,
                                 minimum_band_level: int = None,
                                 maximum_band_level: int = None) -> list:
        """
        Purpose:
            Given a 'slot-name' (such as 'Data Science') return records with a value range
                e.g., evidence of 5 < x < 10
        :param collection_name:
            name of the mongo collection to fetch the data from
        :param slot_name:
            the name of the slot to use in the search
                e.g., 'Data Science' or 'Soft Skill'
                reference:  <https://github.ibm.com/GTS-CDO/unstructured-analytics/
                             blob/master/resources/config/entity_schema_for_dim.yml>
        :param status:
            the open seat status
                draft       a non-published seat
                open        the seat is still open
                withdrawn   the seat was closed without being filled
                closd       the seat was filled and closed
        :param region:
            the region
                ap          Asia Pacific (not China, not Japan)
                eu          Europe (not limited to 'european union')
                gcg         Greater China Group (China, Taiwan, Singapore)
                na          North America (Canada, United States, Mexico)
                jp          Japan
                la          Latin America
                mea         Middle East and Africa
        :param start_date:
            the date to start looking for records
                e.g., format: YYYY-MM-DD
        :param end_date:
            the date to stop looking for records
                e.g., format: YYYY-MM-DD
            if start_date has a value, and end_date is none, then end_date will be set to today's date
        :param minimum_value_sum:
            the minimum threshold of evidence
        :param maximum_value_sum:
            the maximum threshold of evidence
        :param minimum_band_level:
            the minimum band level to search by
        :param maximum_band_level:
            the maximum band level to search by
        :return:
            the requested records
        """
        from cendantdim.runtime.svc import FindDimensionRecordsBySlotForDemand
        finder = FindDimensionRecordsBySlotForDemand(collection_name=collection_name,
                                                     region=region,
                                                     status=status,
                                                     slot_name=slot_name,
                                                     start_date=start_date,
                                                     end_date=end_date,
                                                     mongo_client=self.mongo_client,
                                                     minimum_value_sum=minimum_value_sum,
                                                     maximum_value_sum=maximum_value_sum,
                                                     minimum_band_level=minimum_band_level,
                                                     maximum_band_level=maximum_band_level,
                                                     is_debug=self._is_debug)
        return finder.process()

    def by_slot_value_for_supply(self,
                                 collection_name: str,
                                 slot_name: str,
                                 region: str = None,
                                 minimum_value_sum: float = None,
                                 maximum_value_sum: float = None,
                                 minimum_band_level: int = None,
                                 maximum_band_level: int = None) -> list:
        """
        Purpose:
            Given a 'slot-name' (such as 'Data Science') return records with a value range
                e.g., evidence of 5 < x < 10
        :param collection_name:
            the source collection to search in
                e.g.,  Supply, Demand, Learning (case insensitive)
        :param slot_name:
            the name of the slot to use in the search
                e.g., 'Data Science' or 'Soft Skill'
                reference:  <https://github.ibm.com/GTS-CDO/unstructured-analytics/
                             blob/master/resources/config/entity_schema_for_dim.yml>
        :param region:
        :param minimum_value_sum:
            the minimum threshold of evidence
        :param maximum_value_sum:
            the maximum threshold of evidence
        :param minimum_band_level:
            the minimum band level to search by
        :param maximum_band_level:
            the maximum band level to search by
        :return:
            the requested records
        """
        from cendantdim.runtime.svc import FindDimensionRecordsBySlotForSupply
        finder = FindDimensionRecordsBySlotForSupply(region=region,
                                                     slot_name=slot_name,
                                                     collection_name=collection_name,
                                                     mongo_client=self.mongo_client,
                                                     minimum_value_sum=minimum_value_sum,
                                                     maximum_value_sum=maximum_value_sum,
                                                     minimum_band_level=minimum_band_level,
                                                     maximum_band_level=maximum_band_level,
                                                     is_debug=self._is_debug)
        return finder.process()

    def by_key_field(self,
                     xdm_schema: str,
                     source_name: str,
                     collection_name_tag: str,
                     collection_name_xdm: str,
                     key_field: str = None) -> DataFrame or None:
        """
        Purpose:
            Return dimension records by key-field
        :param xdm_schema:
            the schema to use in the search
            e.g., 'supply' or 'learning'
        :param source_name:
            the dimension manifest source name
        :param collection_name_tag:
            the tag collection name to search
        :param collection_name_xdm:
            the xdm collection name to search
        :param key_field:
            the key field to search by
            e.g., Open Seat ID, Serial Number, Learning Activity ID
        :return:
        """
        from cendantdim.runtime.svc import FindDimensionRecordsByKey
        finder = FindDimensionRecordsByKey(mongo_client=self.mongo_client,
                                           is_debug=self._is_debug)
        return finder.process(key_field=key_field,
                              xdm_schema=xdm_schema,
                              source_name=source_name,
                              collection_name_tag=collection_name_tag,
                              collection_name_xdm=collection_name_xdm)

    def by_key_fields(self,
                      source_name: str,
                      xdm_schema: str,
                      collection_name_tag: str,
                      collection_name_xdm: str,
                      key_fields: list) -> DataFrame:
        """
        Purpose:
            Return dimension records by key-field
        :param source_name:
            the source collection to search in
                e.g.,  Supply, Demand, Learning (case insensitive)
        :param xdm_schema:
            the schema to use in the search
            e.g., 'supply' or 'learning'
        :param collection_name_tag:
            the tag collection name to search
        :param collection_name_xdm:
            the xdm collection name to search
        :param key_fields:
            the key fields to search by
            e.g., Open Seat ID, Serial Number, Learning Activity ID
        :return:
            a DataFrame of evidence for the key-field(s) provided
            +----+------------+----------------------+----------+
            |    |   KeyField | Schema               |   Weight |
            |----+------------+----------------------+----------|
            |  0 |  099711631 | cloud                |       10 |
            |  1 |  099711631 | system administrator |       38 |
            |  2 |  099711631 | database             |        5 |
            |  3 |  099711631 | data science         |        5 |
            |  4 |  099711631 | hard skill           |       52 |
            |  5 |  099711631 | other                |       27 |
            |  6 |  099711631 | soft skill           |       56 |
            |  7 |  099711631 | project management   |       17 |
            |  8 |  099711631 | service management   |        5 |
            +----+------------+----------------------+----------+

            in the event of multiple key-fields being provided, the DataFrame simply becomes larger, e.g.,
            +----+------------+----------------------+----------+
            |    |   KeyField | Schema               |   Weight |
            |----+------------+----------------------+----------|
            |  0 |  053664672 | cloud                |        3 |
            |  1 |  053664672 | system administrator |        3 |
            |  2 |  053664672 | database             |        1 |
            |  3 |  053664672 | data science         |        0 |
            |  4 |  053664672 | hard skill           |        5 |
            |  5 |  053664672 | other                |        5 |
            |  6 |  053664672 | soft skill           |        6 |
            |  7 |  053664672 | project management   |        4 |
            |  8 |  053664672 | service management   |        2 |
            |  9 |  976759672 | cloud                |       18 |
            | 10 |  976759672 | system administrator |       18 |
            | 11 |  976759672 | database             |       11 |
            | 12 |  976759672 | data science         |       29 |
            | 13 |  976759672 | hard skill           |       18 |
            | 14 |  976759672 | other                |       24 |
            | 15 |  976759672 | soft skill           |       23 |
            | 16 |  976759672 | project management   |       11 |
            | 17 |  976759672 | service management   |        7 |
            +----+------------+----------------------+----------+

        """
        from cendantdim.runtime.svc import FindDimensionRecordsByMultiKey
        finder = FindDimensionRecordsByMultiKey(mongo_client=self.mongo_client,
                                                is_debug=self._is_debug)
        return finder.process(source_name=source_name,
                              xdm_schema=xdm_schema,
                              collection_name_tag=collection_name_tag,
                              collection_name_xdm=collection_name_xdm,
                              key_fields=key_fields)
