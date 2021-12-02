#!/usr/bin/env python
# -*- coding: UTF-8 -*-


import pprint
from collections import Counter

import pandas as pd
from pandas import DataFrame

from base import BaseObject
from datadict import FindBadges
from datadict import FindDimensions


class GenerateDataframeWeights(BaseObject):

    def __init__(self,
                 xdm_schema: str,
                 add_provenance_weight: bool = False,
                 add_field_text_weight: bool = False,
                 add_explicit_weights: bool = True,
                 add_implicit_weights: bool = False,
                 add_badge_distribution_weight: bool = True,
                 is_debug: bool = False):
        """
        Created:
            7-May-2019
            craig.trim@ibm.com
            *   refactor functionality out of Jupyter
        Updated:
            7-Aug-2019
            craig.trim@ibm.com
            *   refactor static vs dynamic input to enable caching in API
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/649
        Updated:
            23-Aug-2019
            craig.trim@ibm.com
            *   modify how inferred weights are computed
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/818
        Updated:
            17-Oct-2019
            craig.trim@ibm.com
            *   renamed param 'add-field-name-weight' to 'add-field-text-weight'
                renamed param 'add-collection-weight' to 'add-provenance-weight'
                added param 'add-annotation-weight'
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1142#issuecomment-15374359
        Updated:
            18-Oct-2019
            craig.trim@ibm.com
            *   add badge distribution weights
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/886#issuecomment-15403441
        Updated:
            29-Oct-2019
            craig.trim@ibm.com
            *   remove 'entity-schema-finder' in favor of new approach
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/916#issuecomment-15620801
        :param xdm_schema:
            the name of the schema to perform the type lookup
            Notes:
            -   typically either 'supply' or 'learning'
            -   the full list is on path 'resources/config/dimensionality'
        :param add_provenance_weight:
            add (or remove) additional weight based on the MongoDB collection provenance for the annotation (tag)
                e.g.    'ingest_cv_career_history' implies prior work experience
                        'ingest_cv_skills_profile' is a general skills listing
        :param add_field_text_weight:
            add (or remove) additional weight based on the MongoDB field name for the annotation (tag)
                e.g.    'badge' field
        :param add_explicit_weights
            add (or remove) additional weight based on explicit tags
        :param add_implicit_weights:
            add (or remove) additional weight based on inferred tags
        :param add_badge_distribution_weight:
            lower weights for popular and easily attainable badges
        :param is_debug:
        """
        BaseObject.__init__(self, __name__)
        from nlusvc.core.dmo import AnnotationWeight
        from nlusvc.core.dmo import FieldTextWeight
        from nlusvc.core.dmo import CollectionNameWeight

        self._is_debug = is_debug
        self._add_explicit_weights = add_explicit_weights
        self._add_implicit_weights = add_implicit_weights
        self._add_field_text_weight = add_field_text_weight
        self._add_provenance_weight = add_provenance_weight
        self._add_badge_distribution_weight = add_badge_distribution_weight

        self._dim_finder = FindDimensions(xdm_schema, is_debug=is_debug)
        self._text_weight = FieldTextWeight(is_debug=is_debug)
        self._badge_finder = FindBadges(is_debug=self._is_debug)
        self._annotation_weight = AnnotationWeight(is_debug=self._is_debug)
        self._provenance_weight = CollectionNameWeight(is_debug=is_debug)

    def _to_dataframe(self,
                      d_weights: dict) -> DataFrame:
        results = []
        for k in self._dim_finder.top_level_entities():
            results.append({"Schema": k,
                            "Weight": sum(d_weights[k])})

        return pd.DataFrame(results)

    def _find_weight(self,
                     collection: str,
                     field_name: str,
                     tags: list,
                     original_text: str) -> float:

        def _collection_weight() -> float:
            if self._add_provenance_weight:
                return self._provenance_weight.process(collection)
            return 1.0

        def _field_text_weight() -> float:
            if self._add_field_text_weight:
                return self._text_weight.process(field_text=original_text)
            return 1.0

        def _tag_weight() -> float:
            if self._add_explicit_weights:
                return sum([self._annotation_weight.process(tag) for tag in tags])
            return 1.0

        def _badge_weight() -> float:
            if self._add_badge_distribution_weight:
                return self._badge_finder.weight_deduction_by_badge(original_text)
            return 0.0

        base_weight = 1.0
        tag_weight = _tag_weight()
        badge_weight = _badge_weight()
        collection_weight = _collection_weight()
        field_name_weight = _field_text_weight()
        total_weight = (base_weight * collection_weight * field_name_weight * tag_weight) - badge_weight
        if total_weight < 0:
            total_weight = 0.1

        if self._is_debug:
            self.logger.debug('\n'.join([
                "Rule Weight Computation:",

                f"\tProvenance ("
                f"collection={collection}, "
                f"field-name={field_name}, "
                f"text={original_text})",

                f"\tWeights ("
                f"base={base_weight}, "
                f"badge={badge_weight}, "
                f"collection={collection_weight}, "
                f"tags={tag_weight}, "
                f"field={field_name_weight}, "
                f"total={total_weight})"]))

        return total_weight

    @staticmethod
    def _transform(df_evidence: DataFrame,
                   df_inference: DataFrame) -> []:
        """
        Purpose:
            Merge (and summarize) both incoming DataFrames into a single dictionary
        Rationale:
            Optimize information for Rules-based processing
        Reference:
            https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/818#issuecomment-14070410
        :return:
            a merged dictionary
        """
        results = []

        for original_text in df_evidence['OriginalText'].unique():
            df2 = df_evidence[df_evidence['OriginalText'] == original_text]
            field_name = list(df2['FieldName'].unique())[0]
            collection = list(df2['Collection'].unique())[0]

            explicit_schema_counter = Counter()
            for explicit_schema in df2['Schema'].unique():

                explicit_schema_counter.update({explicit_schema: 1})

                df3 = df2[df2['Schema'] == explicit_schema]
                confidence = df3['TagScore'].unique()[0]

                implicit_schema_counter = Counter()

                for tag in df3['Tag'].unique():
                    df_infer_1 = df_inference[df_inference['ExplicitTag'] == tag]
                    df_infer_2 = df_inference[df_inference['ImplicitTag'] == tag]

                    schemas = []
                    schemas += list(df_infer_1['ExplicitSchema'].unique())
                    schemas += list(df_infer_1['ImplicitSchema'].unique())
                    schemas += list(df_infer_2['ExplicitSchema'].unique())
                    schemas += list(df_infer_2['ImplicitSchema'].unique())

                    [implicit_schema_counter.update({x: 1}) for x in schemas]

                results.append({
                    "OriginalText": original_text,
                    "FieldName": field_name,
                    "Collection": collection,
                    "Confidence": confidence,
                    "Tags": sorted(df3['Tag'].unique()),
                    "ExplicitSchemas": dict(explicit_schema_counter),
                    "ImplicitSchemas": dict(implicit_schema_counter)})

        return results

    def process(self,
                df_evidence: DataFrame,
                df_inference: DataFrame) -> DataFrame:
        """
        :param df_evidence:
            a pandas DataFrame containing explicit tags
                e.g.    an explicit tag is an annotation present in the unstructured text
        :param df_inference:
            a pandas DataFrame containing implicit (inferred) tags
                e.g.    an inferred tag has a semantic relationship to either an explicit tag
                        or another inferred tag
        :return:
        """
        d_weights = {}

        for schema in self._dim_finder.top_level_entities():
            if schema not in d_weights:
                d_weights[schema] = []

        results = self._transform(df_evidence=df_evidence,
                                  df_inference=df_inference)

        if self._is_debug:
            self.logger.debug('\n'.join([
                "Transformed Incoming DataFrames",
                pprint.pformat(results, indent=4)]))

        for result in results:
            rule_weight = self._find_weight(collection=result["Collection"],
                                            field_name=result["FieldName"],
                                            tags=result["Tags"],
                                            original_text=result["OriginalText"])

            confidence_weight = float(result["Confidence"]) / 100

            def apply_to_schema(schema_type: str,
                                schema_weight: float) -> None:

                for a_schema in result[schema_type]:
                    count = result[schema_type][a_schema]
                    total_weight = rule_weight * count * confidence_weight * schema_weight

                    if a_schema not in d_weights:
                        d_weights[a_schema] = []
                    d_weights[a_schema].append(total_weight)

                    if self._is_debug:
                        self.logger.debug('\n'.join([
                            "Final Weighting:",

                            f"\tSchema ("
                            f"name={a_schema}, type={schema_type})",

                            f"\tWeights ("
                            f"schema={schema_weight}, "
                            f"count={count}, "
                            f"rule={rule_weight}, "
                            f"confidence={confidence_weight}, "
                            f"total={round(total_weight, 2)})"]))

            apply_to_schema(schema_type="ExplicitSchemas",
                            schema_weight=1.0)

            apply_to_schema(schema_type="ImplicitSchemas",
                            schema_weight=0.2)

        return self._to_dataframe(d_weights)
