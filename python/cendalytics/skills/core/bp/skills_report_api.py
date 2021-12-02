# !/usr/bin/env python
# -*- coding: UTF-8 -*-


from typing import Optional, Union

from graphviz import Digraph
from pandas import DataFrame

from base import BaseHash
from base import BaseObject
from cendalytics.skills.core.svc import FindSelfReportedCertifications
from cendalytics.skills.core.svc import GenerateCsvReport
from cendalytics.skills.core.svc import GenerateDivisionDistribution
from cendalytics.skills.core.svc import PerformKeyFieldSearch
from cendalytics.skills.core.svc import PerformNormalizedTextSearch
from cendalytics.skills.core.svc import PerformTagSearch
from datadict import FindEntity
from datadict import FindSynonym
from nlusvc import OntologyAPI
from nlutext import NormalizeIncomingText


class SkillsReportAPI(BaseObject):
    """
    Purpose:
    Expose an API for Reporting Usage in Jupyter

    This API is not meant to be an 'all-in-all' API that handles all aspects of data I/O
    but primarily providing service mechanisms to manipulate in-flight data

    Traceability:
    https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/141
    """

    def __init__(self,
                 ontology_name: str = 'base',
                 is_debug: bool = False):
        """
        Created:
            8-Apr-2019
            craig.trim@ibm.com
            *   https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/71
        Updated:
            16-Sept-2019
            craig.trim@ibm.com
            *   update search API
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/940
        Updated:
            31-Oct-2019
            craig.trim@ibm.co
            *   deprecate 'search' in favor of 'search-on-text'
                add API for 'search-on-tags'
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1224#issuecomment-15673306
        Updated:
            8-Nov-2019
            craig.trim@ibm.com
            *   Update with Facade Pattern
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1293
        Updated:
            13-Dec-2019
            craig.trim@ibm.com
            *   load dictionaries by ontology name
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1582
        Updated:
            13-Feb-2020
            craig.trim@ibm.com
            *   Pass in Server Alias as a parameter to 'search' function
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1855
        """
        BaseObject.__init__(self, __name__)
        from cendalytics.skills.core.dto import SkillsReportValidator

        self._is_debug = is_debug
        self._validator = SkillsReportValidator

        self._entity_finder = FindEntity(is_debug=is_debug,
                                         ontology_name=ontology_name)
        self._synonym_finder = FindSynonym(is_debug=is_debug,
                                           ontology_name=ontology_name)

    def report(self,
               collection_name: str,
               mongo_database_name: str = 'cendant'):
        """ Report Facade
        :return:
            The Facade returns an instantiated Service for the consumer to invoke.
            The Facade *does not* invoke the Service itself.
        """
        from cendalytics.skills.core.fcd import SkillsReportOnCertifications
        from cendalytics.skills.core.fcd import SkillsReportOnDivisionDistribution

        class Facade(object):

            @staticmethod
            def divisions() -> GenerateDivisionDistribution:
                return SkillsReportOnDivisionDistribution(collection_name=collection_name,
                                                          is_debug=self._is_debug).initialize()

            @staticmethod
            def self_reported_certifications(exclude_vendors: Optional[list],
                                             add_normalized_text: bool = True,
                                             aggregate_data: bool = False) -> FindSelfReportedCertifications:
                return SkillsReportOnCertifications(exclude_vendors=exclude_vendors,
                                                    aggregate_data=aggregate_data,
                                                    add_normalized_text=add_normalized_text,
                                                    collection_name=collection_name,
                                                    mongo_database_name=mongo_database_name,
                                                    is_debug=self._is_debug).initialize()

        return Facade()

    def search(self,
               collection_name: str,
               key_field: str = None,
               server_alias: str = 'cloud'):
        """ Search Facade
        :return:
            The Facade returns an instantiated Service for the consumer to invoke.
            The Facade *does not* invoke the Service itself.
        """
        from cendalytics.skills.core.fcd import SkillsSearchOnTags
        from cendalytics.skills.core.fcd import SkillsSearchOnKeyField
        from cendalytics.skills.core.fcd import SkillsSearchOnNormalizedText

        class Facade(object):

            @staticmethod
            def key_field() -> PerformKeyFieldSearch:
                return SkillsSearchOnKeyField(key_field=key_field,                          # type: ignore
                                              is_debug=self._is_debug,                      # key_field is not optional
                                              collection_name=collection_name).initialize()

            @staticmethod
            def tags(tags: list,
                     div_field: Union[str, list]) -> PerformTagSearch:
                return SkillsSearchOnTags(tags=tags,                                        # type: ignore
                                          collection_name=collection_name,                  # key_field is not optional
                                          div_field=div_field,
                                          key_field=key_field,
                                          is_debug=self._is_debug).initialize()

            @staticmethod
            def normalized_text(search_terms: list = None,
                                div_field: Optional[Union[str, list]] = None) -> PerformNormalizedTextSearch:
                return SkillsSearchOnNormalizedText(div_field=div_field,
                                                    key_field=key_field,
                                                    search_terms=search_terms,
                                                    collection_name=collection_name,
                                                    is_debug=self._is_debug).initialize()

        return Facade()

    def utility(self):
        """ Report Facade
        :return:
            The Facade returns an instantiated Service for the consumer to invoke.
            The Facade *does not* invoke the Service itself.
        """
        from cendalytics.skills.core.fcd import SkillsUtilityToCSV

        class Facade(object):

            @staticmethod
            def to_csv(df_results: DataFrame,
                       hash_serial_number: bool = False) -> GenerateCsvReport:
                return SkillsUtilityToCSV(df_results=df_results,
                                          hash_serial_number=hash_serial_number,
                                          is_debug=self._is_debug).initialize()

            @staticmethod
            def variants(token: str) -> list:
                """
                :param token:
                    any input token
                    ** should be the normalized form **
                :return:
                    a list of zero-or-more variations of this same term
                """
                self._validator.variants(token)
                return self._synonym_finder.synonyms(token)

            @staticmethod
            def normalize(token: str) -> str:
                """
                :param token:
                    any input token
                :return:
                    the same input token normalized against the Cendant Ontology
                """
                self._validator.normalize(token)
                svcresult = NormalizeIncomingText().process(token)
                normalized = svcresult["normalized"].replace("_", " ")
                normalized = self._entity_finder.label_or_self(normalized)

                return normalized

            @staticmethod
            def hash_serial_number(df: DataFrame,
                                   column_name: str = "SerialNumber") -> DataFrame:
                self._validator.hash_serial_number(df, column_name)
                df["SerialNumberHash"] = [BaseHash.serial_number(x) for x in df[column_name]]
                return df

            @staticmethod
            def graphviz(df: DataFrame,
                         engine: str = "dot") -> Digraph:
                """
                :param df:
                    OntologyAPI.relationships(...)
                :param engine:
                     'dot'      creates a hierarchy, suitable for smaller graphs
                                where parent/child detail is important
                     'fdp'      creates a clustered graph
                                where 'whole graph' patterns are essential
                :return:
                    a Graphviz digrrah
                """
                self._validator.graphviz(df, engine)
                return OntologyAPI.graphviz(df=df,
                                            graph_style="nlp",
                                            engine=engine)

        return Facade()

    # *********************************************************************************
    # DEPRECATED METHODS FOR UTILITY FUNCTIONS
    # The Skills Report API is used extensively in Jupyter, and we must support existing APIs
    # *********************************************************************************

    def graphviz(self, df: DataFrame, engine: str = "dot") -> Digraph:
        return self.utility().graphviz(df=df, engine=engine)

    def to_csv(self, df_results: DataFrame, hash_serial_number: bool = False) -> DataFrame:
        return self.utility().to_csv(df_results=df_results, hash_serial_number=hash_serial_number).process()

    def normalize(self, token: str) -> str:
        return self.utility().normalize(token)

    def variants(self, token: str) -> list:
        return self.utility().variants(token)

    def hash_serial_number(self, df: DataFrame, column_name: str = "SerialNumber") -> DataFrame:
        return self.utility().hash_serial_number(df=df, column_name=column_name)

    # *********************************************************************************
    # DEPRECATED METHODS FOR SEARCHING
    # The Skills Report API is used extensively in Jupyter, and we must support existing APIs
    # *********************************************************************************

    def search_on_tags(self,
                       tags: list,
                       collection_name: str,
                       div_field: Union[str, list],
                       key_field: str,
                       host: str = 'wftag') -> DataFrame:
        searcher = self.search(key_field=key_field,
                               collection_name=collection_name)

        return searcher.tags(tags=tags,
                             div_field=div_field).process()

    def search_on_key_field(self,
                            collection_name: str,
                            key_field: str = None) -> DataFrame:
        searcher = self.search(key_field=key_field,
                               collection_name=collection_name)

        return searcher.key_field().process()

    def search_on_normalized_text(self,
                                  collection_name: str,
                                  search_terms: list = None,
                                  div_field: Optional[Union[str, list]] = None,
                                  key_field: str = None) -> DataFrame:
        searcher = self.search(key_field=key_field,
                               collection_name=collection_name)

        return searcher.normalized_text(search_terms=search_terms,
                                        div_field=div_field).process()

    # *********************************************************************************
    # DEPRECATED METHODS FOR REPORTING
    # The Skills Report API is used extensively in Jupyter, and we must support existing APIs
    # *********************************************************************************

    def division_distribution(self, collection_name: str) -> DataFrame:
        return self.report(collection_name=collection_name).divisions().process()

    def self_reported_certifications(self,
                                     collection_name: str,
                                     exclude_vendors: Optional[list],
                                     add_normalized_text: bool = True,
                                     aggregate_data: bool = False,
                                     mongo_database_name: str = 'cendant') -> DataFrame:
        self.logger.debug('\n'.join([
            "Invoked Self-Reported Certifications API Function",
            f"\tCollection Name: {collection_name}",
        ]))

        reporter = self.report(collection_name=collection_name,
                               mongo_database_name=mongo_database_name)

        return reporter.self_reported_certifications(exclude_vendors=exclude_vendors,
                                                     add_normalized_text=add_normalized_text,
                                                     aggregate_data=aggregate_data).process()
