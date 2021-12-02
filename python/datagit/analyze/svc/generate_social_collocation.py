# !/usr/bin/env python
# -*- coding: UTF-8 -*-


import os
from collections import Counter
from typing import Optional

import pandas as pd
from pandas import DataFrame

from base import BaseObject
from datagit.analyze.dmo import CommentMentionExtractor
from datagit.analyze.dmo import SocialNameNormalizer
from datamongo import CendantCollection
from datamongo import CendantRecordParser


class GenerateSocialCollocation(BaseObject):
    """ Generate a Collocation Analysis of Social Relationships """

    def __init__(self,
                 collection_name: str,
                 is_debug: bool = True):
        """
        Created:
            30-Dec-2019
            craig.trim@ibm.com
            *   https://github.ibm.com/-cdo/unstructured-analytics/issues/1680
        Updated:
            2-Jan-2020
            craig.trim@ibm.com
            *   generate an entity file along with the relationships file
                https://github.ibm.com/-cdo/unstructured-analytics/issues/1680#issuecomment-16901010
        :param is_debug:
            if True     increase log output at DEBUG level
        """
        BaseObject.__init__(self, __name__)
        self._is_debug = is_debug
        self._collection = self._load_collection(collection_name)

        self._entities = Counter()  # count entities (Jane, Jim)
        self._relationships = Counter()  # count relationships (Jane -> Jim)

    def _load_collection(self,
                         collection_name) -> CendantCollection:
        return CendantCollection(is_debug=self._is_debug,
                                 some_collection_name=collection_name)

    def _div_field_by_name(self,
                           a_field_name: str) -> list:
        records = self._collection.by_field("div_field", a_field_name)

        if not records:
            self.logger.error('\n'.join([
                "No Records Found",
                f"\tDiv Field: {a_field_name}"]))
            raise ValueError

        self.logger.debug('\n'.join([
            "Retrieved Records",
            f"\tDiv Field: {a_field_name}",
            f"\tTotal: {len(records)}"]))

        return records

    @staticmethod
    def _cartesian(some_values: list) -> list:
        """
        Purpose:
            Generate a Cartesian Product of a Series of Lists
        Implementation:
            1.  Given a Series of Lists
                [ [44, 12, 8], [49], [47] ]
            2.  Generate a Cartesian Product
                [(44, 49, 47), (12, 49, 47), (8, 49, 47)]
        Reference:
            https://stackoverflow.com/questions/533905/get-the-cartesian-product-of-a-series-of-lists
        :param some_values:
            a list of avlues
        :return:
            a Cartesian Product of the input values
        """
        s = set()

        for x in some_values:
            for y in some_values:
                if x != y:
                    s.add(','.join(sorted({x, y})))

        return sorted(s)

    def _analyze_comments(self,
                          comments: list) -> None:

        def mentions(a_record: dict) -> list:
            return CommentMentionExtractor(d_record=a_record,
                                           is_debug=self._is_debug).process()

        def normalize(input_text: str) -> str:
            return SocialNameNormalizer(input_text=input_text,
                                        is_debug=self._is_debug).process()

        def open_by(a_comment: dict) -> str:
            parser = CendantRecordParser(is_debug=self._is_debug)
            return parser.field_value_by_name(a_comment, 'OpenBy')

        for comment in comments:
            names = set()

            names.add(normalize(open_by(comment)))
            [names.add(name) for name in mentions(comment)]
            names = [name.lower().strip() for name in names if name]

            # add entities (individual names; Jane, Jim)
            [self._entities.update({name.lower(): 1}) for name in names]

            # add relationships (Jane -> Jim)
            if len(names) >= 2:
                [self._relationships.update({pattern.lower(): 1})
                 for pattern in self._cartesian(sorted(names))]

    def _analyze_issues(self,
                        issues: list) -> None:

        parser = CendantRecordParser(is_debug=self._is_debug)

        def normalize(input_text: str) -> Optional[str]:
            if input_text:
                return SocialNameNormalizer(input_text=input_text,
                                            is_debug=self._is_debug).process()

        def open_by(a_comment: dict) -> str:
            return parser.field_value_by_name(a_comment, 'OpenBy')

        def close_by(a_comment: dict) -> str:
            return parser.field_value_by_name(a_comment, 'CloseBy')

        def assignees(a_comment: dict) -> str:
            return parser.field_value_by_name(a_comment, 'Assignees')

        for issue in issues:
            names = set()

            names.add(normalize(open_by(issue)))
            names.add(normalize(close_by(issue)))
            names.add(normalize(assignees(issue)))
            names = [name for name in names if name and len(name)]
            names = [name.lower().strip() for name in names if name]

            # add entities (individual names; Jane, Jim)
            [self._entities.update({name.lower(): 1}) for name in names]

            # add relationships (Jane -> Jim)
            if len(names) >= 2:
                [self._relationships.update({pattern.lower(): 1})
                 for pattern in self._cartesian(sorted(names))]

    @staticmethod
    def _to_dataframe(a_counter: Counter) -> DataFrame:
        results = []
        for x in a_counter:
            results.append({
                "Name": x,
                "Count": a_counter[x]})
        return pd.DataFrame(results)

    def _write_to_file(self,
                       df_result: DataFrame,
                       output_path: str):
        output_path = os.path.join(os.environ['CODE_BASE'], output_path)
        df_result.to_csv(output_path, encoding='utf-8', sep='\t')
        if self._is_debug:
            self.logger.debug('\n'.join([
                "Wrote Result to File",
                f"\tPath: {output_path}",
                f"\tTotal Records: {len(df_result)}"]))

    def process(self,
                write_to_file: bool = True,
                output_rel_relative_path: Optional[
                    str] = 'resources/output/analysis/graphviz_socialrel_collocation.csv',
                output_ent_relative_path: Optional[
                    str] = 'resources/output/analysis/graphviz_socialent_collocation.csv') -> dict:
        """
        Purpose:
            Perform Social Collocation Analysis
        :param write_to_file:
            True        write results to file
            False       do not persist
        :param output_rel_relative_path:
            Optional    the path to write the relationship analys to
        :param output_ent_relative_path:
            Optional    the path to write the entity analysis to
        :return:
            a dictionary of results
        """
        self._analyze_comments(self._div_field_by_name("comment"))
        self._analyze_issues(self._div_field_by_name("issue"))

        df_ent = self._to_dataframe(self._entities)
        df_rel = self._to_dataframe(self._relationships)

        if self._is_debug:
            self.logger.debug('\n'.join([
                "Collocation Analysis Completed",
                f"\tTotal Entities: {len(df_ent)}",
                f"\tTotal Relationships: {len(df_rel)}"]))

        if write_to_file and output_rel_relative_path and output_ent_relative_path:
            self._write_to_file(df_ent, output_ent_relative_path)
            self._write_to_file(df_rel, output_rel_relative_path)

        return {
            "ent": df_ent,
            "rel": df_rel}
