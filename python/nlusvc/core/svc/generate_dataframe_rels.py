#!/usr/bin/env python
# -*- coding: UTF-8 -*-


import time

import pandas as pd
from pandas import DataFrame
from tabulate import tabulate

from base import BaseObject
from datadict import FindDimensions
from datadict import FindEntity
from datadict import FindPatterns
from datadict import FindRelationships
from datadict import FindSynonym
from nlutext import NormalizeIncomingText


class GenerateDataFrameRels(BaseObject):
    """ """

    __text_normalizer = None

    def __init__(self,
                 xdm_schema: str,
                 add_tag_syns: bool = True,
                 add_tag_rels: bool = True,
                 add_rel_syns: bool = False,
                 add_rel_owns: bool = True,
                 add_wiki_references: bool = False,
                 filter_on_key_terms: bool = False,
                 inference_level: int = 0,
                 ontology_name: str = 'base',
                 is_debug: bool = False):
        """
        Created:
            7-May-2019
            craig.trim@ibm.com
            *   refactored functionality out of a Jupyter notebook
        Updated:
            9-May-2019
            craig.trim@ibm.com
            *   updated attribute retrieval
        Updated:
            15-May-2019
            craig.trim@ibm.com
            *   added implication tag check to prevent None values in DataFrame
        Updated:
            15-Jul-2019
            craig.trim@ibm.com
            *   added 'see-also-tags' to variation generator
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/435
            *   filter-on-key-terms must be optional (flag)
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/435#issuecomment-12693744
            *   added 'inference-level'
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/435#issuecomment-12695441
        Updated:
            7-Aug-2019
            craig.trim@ibm.com
            *   removed tag normalization - high processing times
                https://github.ibm.com/-cdo/unstructured-analytics/issues/661
        Updated:
            8-Oct-2019
            craig.trim@ibm.com
            *   add an additional parameter to the inference-api
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1066
        Updated:
            29-Oct-2019
            craig.trim@ibm.com
            *   remove 'entity-schema-finder' in favor of new approach
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/916#issuecomment-15620801
        Updated:
            31-Oct-2019
            craig.trim@ibm.com
            *   remove automated child node addition
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1230#issuecomment-15676863
        Updated:
            13-Dec-2019
            craig.trim@ibm.com
            *   load dictionaries by ontology name
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1582
        :param xdm_schema:
            the name of the schema to perform the type lookup
            Notes:
            -   typically either 'supply' or 'learning'
            -   the full list is on path 'resources/config/dimensionality'
        :param add_tag_syns:
            if True     add synonyms (all language variability) for incoming tag set
        :param add_tag_rels:
            if True     add relationships (from Cendant Ontology) for all incoming tag set
        :param add_rel_syns:
            if True     add synonyms for related terms
        :param add_rel_owns:
            if True     add relationships from 'a owns b'
                        these tend to be noisier than the inverse
                            'b owned-by a'
                        in the inverse case, only one result is typically returned
                        use of the 'a owns b' relationship can return dozens (or even 100s) of results
        :param inference_level:
            number of transitive hops to perform
            Given:
                a is-rel b is-rel c is-rel d is-rel e is-rel f
            Given 'a' and inference_level = 0
                returns [a]
            Given 'a' and inference_level = 1
                returns [a, b]
            Given 'a' and inference_level = 2
                returns [a, b, c]
            ...
            Given 'a' and inference_level = 5
                returns [a, b, c, d, e, f]
            ...
            Given 'a' and inference_level = 100
                returns [a, b, c, d, e, f]

            default=0;          implies no inference is performed
            no upper limit;     if the param value is greater than the number of inferential routes
                                all inferential routes will be returned

        :param add_wiki_references:
            if True     add references from dbPedia (Wikipedia Lookups)
        :param is_debug:
            if Tru      additional logging
        """
        BaseObject.__init__(self, __name__)
        start = time.time()

        self._is_debug = is_debug

        self.add_tag_syns = add_tag_syns
        self.add_tag_rels = add_tag_rels
        self.add_rel_syns = add_rel_syns
        self.add_rel_owns = add_rel_owns
        self.inference_level = inference_level
        self.add_wiki_references = add_wiki_references
        self.filter_on_key_terms = filter_on_key_terms

        self.pattern_finder = FindPatterns(is_debug=is_debug,
                                           ontology_name=ontology_name)
        self.entity_finder = FindEntity(is_debug=is_debug,
                                        ontology_name=ontology_name)
        self.synonym_finder = FindSynonym(is_debug=is_debug,
                                          ontology_name=ontology_name)
        self.rel_finder = FindRelationships(is_debug=is_debug,
                                            ontology_name=ontology_name)
        self._dim_finder = FindDimensions(schema=xdm_schema,
                                          is_debug=is_debug,
                                          ontology_name=ontology_name)

        total_time = time.time() - start
        if self._is_debug:
            self.logger.debug('\n'.join([
                f"Initialized GenerateDataframeRels (time={total_time}s)",
                f"\tOntology Name: {ontology_name}"]))

    def _normalize(self,
                   token: str) -> str:
        """
        :param token:
            any input token
        :return:
            the same input token normalized against the Cendant Ontology
        """
        if not self.__text_normalizer:
            self.__text_normalizer = NormalizeIncomingText()

        svcresult = self.__text_normalizer.process(token)
        normalized = svcresult["normalized"].replace("_", " ")
        normalized = self.entity_finder.label(normalized)

        return normalized

    def _synonyms(self,
                  some_input: str) -> list:
        results = self.synonym_finder.synonyms(some_input)
        if not results:
            return []
        return results

    def _lookup(self,
                inference_counter: int,
                an_existing_tag: str,
                original_tags: list,
                s_unique: set,
                results: list) -> None:
        if not an_existing_tag:
            return

        s_unique.add(an_existing_tag)

        if self.add_tag_rels:

            # [self._add(inference_counter=inference_counter + 1,
            #            an_existing_tag=an_existing_tag,
            #            a_relationship_name="Implies",
            #            an_implied_tag=x,
            #            original_tags=original_tags,
            #            s_unique=s_unique,
            #            results=results) for x in self.rel_finder.implies(an_existing_tag,
            #                                                              common_implications=True)]

            # [self._add(inference_counter=inference_counter + 1,
            #            an_existing_tag=an_existing_tag,
            #            a_relationship_name="ImpliedBy",
            #            an_implied_tag=x,
            #            original_tags=original_tags,
            #            s_unique=s_unique,
            #            results=results) for x in self.rel_finder.implied_by(an_existing_tag)]

            # [self._add(inference_counter=inference_counter + 1,
            #            an_existing_tag=an_existing_tag,
            #            a_relationship_name="Requires",
            #            an_implied_tag=x,
            #            original_tags=original_tags,
            #            s_unique=s_unique,
            #            results=results) for x in self.rel_finder.requires(an_existing_tag)]
            #
            # [self._add(inference_counter=inference_counter + 1,
            #            an_existing_tag=an_existing_tag,
            #            a_relationship_name="RequiredBy",
            #            an_implied_tag=x,
            #            original_tags=original_tags,
            #            s_unique=s_unique,
            #            results=results) for x in self.rel_finder.required_by(an_existing_tag)]

            [self._add(inference_counter=inference_counter + 1,
                       an_existing_tag=an_existing_tag,
                       a_relationship_name="PartOf",
                       an_implied_tag=x,
                       original_tags=original_tags,
                       s_unique=s_unique,
                       results=results) for x in self.rel_finder.part_of(an_existing_tag)]

            # [self._add(inference_counter=inference_counter + 1,
            #            an_existing_tag=an_existing_tag,
            #            a_relationship_name="OwnedBy",
            #            an_implied_tag=x,
            #            original_tags=original_tags,
            #            s_unique=s_unique,
            #            results=results) for x in self.rel_finder.owned_by(an_existing_tag)]

            [self._add(inference_counter=inference_counter + 1,
                       an_existing_tag=an_existing_tag,
                       a_relationship_name="Produces",
                       an_implied_tag=x,
                       original_tags=original_tags,
                       s_unique=s_unique,
                       results=results) for x in self.rel_finder.produces(an_existing_tag)]

            [self._add(inference_counter=inference_counter + 1,
                       an_existing_tag=an_existing_tag,
                       a_relationship_name="ProducedBy",
                       an_implied_tag=x,
                       original_tags=original_tags,
                       s_unique=s_unique,
                       results=results) for x in self.rel_finder.producedBy(an_existing_tag)]

            [self._add(inference_counter=inference_counter + 1,
                       an_existing_tag=an_existing_tag,
                       a_relationship_name="RunsOn",
                       an_implied_tag=x,
                       original_tags=original_tags,
                       s_unique=s_unique,
                       results=results) for x in self.rel_finder.runsOn(an_existing_tag)]

            # if self.add_rel_owns:
            #     _owns_references = self.rel_finder.owns(an_existing_tag,
            #                                             include_children=True)
            #     if len(_owns_references) < 25:
            #         [self._add(inference_counter=inference_counter + 1,
            #                    an_existing_tag=an_existing_tag,
            #                    a_relationship_name="Owns",
            #                    an_implied_tag=x,
            #                    original_tags=original_tags,
            #                    s_unique=s_unique,
            #                    results=results) for x in _owns_references]

            [self._add(inference_counter=inference_counter + 1,
                       an_existing_tag=an_existing_tag,
                       a_relationship_name="SimilarTo",
                       an_implied_tag=x,
                       original_tags=original_tags,
                       s_unique=s_unique,
                       results=results) for x in self.rel_finder.similar_to(an_existing_tag)]

            [self._add(inference_counter=inference_counter + 1,
                       an_existing_tag=an_existing_tag,
                       a_relationship_name="Parent",
                       an_implied_tag=x,
                       original_tags=original_tags,
                       s_unique=s_unique,
                       results=results) for x in self.rel_finder.parents(an_existing_tag)]

            # _child_references = self.rel_finder.parent_of(an_existing_tag)
            # if len(_child_references) < 10:
            #     [self._add(inference_counter=inference_counter + 1,
            #                an_existing_tag=an_existing_tag,
            #                a_relationship_name="Child",
            #                an_implied_tag=x,
            #                original_tags=original_tags,
            #                s_unique=s_unique,
            #                results=results) for x in _child_references]

            if self.add_wiki_references:
                [self._add(inference_counter=inference_counter + 1,
                           an_existing_tag=an_existing_tag,
                           a_relationship_name="References",
                           an_implied_tag=x,
                           original_tags=original_tags,
                           s_unique=s_unique,
                           results=results) for x in self.rel_finder.references_transitive(an_existing_tag)]

        # if self._add_child_terms:
        #     [set.add(x) for x in self.rel_finder.children(tag)]

        def _add_variants() -> bool:
            is_original_tag = an_existing_tag in original_tags
            if is_original_tag and self.add_tag_syns:
                return True
            if not is_original_tag and self.add_rel_syns:
                return True

            return False

        if _add_variants():
            for schema in self._dim_finder.find(an_existing_tag):
                for implied_tag in self._synonyms(an_existing_tag):
                    self._add(inference_counter=inference_counter + 1,
                              an_existing_tag=an_existing_tag,
                              a_relationship_name="Variant",
                              an_implied_tag=implied_tag,
                              an_existing_tag_schema=schema,
                              an_implied_tag_schema=schema,
                              original_tags=original_tags,
                              s_unique=s_unique,
                              results=results)

            see_also_tags = self.pattern_finder.find(an_existing_tag, include_patterns=False)
            if see_also_tags:
                see_also_tags = [x for x in see_also_tags if x != an_existing_tag]
                for see_also_tag in see_also_tags:
                    for schema in self._dim_finder.find(see_also_tag):
                        for implied_tag in self._synonyms(see_also_tag):
                            self._add(inference_counter=inference_counter + 1,
                                      an_existing_tag=see_also_tag,
                                      a_relationship_name="Variant",
                                      an_implied_tag=implied_tag,
                                      an_existing_tag_schema=schema,
                                      an_implied_tag_schema=schema,
                                      original_tags=original_tags,
                                      s_unique=s_unique,
                                      results=results)

    def _add(self,
             inference_counter: int,
             an_existing_tag: str,
             an_implied_tag: str,
             a_relationship_name: str,
             original_tags: list,
             s_unique: set,
             results: list,
             an_existing_tag_schema: str = None,
             an_implied_tag_schema: str = None) -> None:

        def _existing_tag_schemas() -> list:
            if an_existing_tag_schema:
                return [an_existing_tag_schema]
            return self._dim_finder.find(an_existing_tag)

        def _implied_tag_schemas() -> list:
            if an_implied_tag_schema:
                return [an_implied_tag_schema]
            if an_implied_tag != an_existing_tag:
                return self._dim_finder.find(an_implied_tag)

        def _is_primary():
            return an_existing_tag in original_tags

        def _is_variant():
            return a_relationship_name == "Variant"

        def _existing_tag():
            return self.entity_finder.label_or_self(an_existing_tag)

        def _implied_tag():
            if an_implied_tag != an_existing_tag:
                return self.entity_finder.label_or_self(an_implied_tag)

        existing_tag = _existing_tag()
        existing_schemas = _existing_tag_schemas()
        implied_tag = _implied_tag()
        implied_schemas = _implied_tag_schemas()

        if not implied_tag or not existing_schemas or not len(existing_schemas):  # 15-May-2019

            # Common Situation
            if an_existing_tag == an_implied_tag:
                return

            # Unexpected Situation
            self.logger.warning("\n".join([
                "Implication Not Found",
                "\tExplicit Tag: {} -> {}".format(an_existing_tag, existing_tag),
                "\tExplicit Schema: {} -> {}".format(an_existing_tag_schema, existing_schemas),
                "\tImplicit Tag: {} -> {}".format(an_implied_tag, implied_tag),
                "\tImplicit Schema: {} -> {}".format(an_implied_tag_schema, implied_schemas),
                "\tRelationship: {}".format(a_relationship_name)]))

            return

        if _existing_tag() != _implied_tag():
            for existing_schema in existing_schemas:
                for implied_schema in implied_schemas:
                    results.append({
                        "ExplicitTag": existing_tag,
                        "ExplicitSchema": existing_schema,
                        "Relationship": a_relationship_name,
                        "ImplicitTag": implied_tag,
                        "ImplicitSchema": implied_schema,
                        "IsPrimary": _is_primary(),
                        "IsVariant": _is_variant()})

        if an_implied_tag and an_implied_tag not in s_unique:
            if inference_counter <= self.inference_level:
                self._lookup(inference_counter=inference_counter,
                             an_existing_tag=an_implied_tag,
                             original_tags=original_tags,
                             s_unique=s_unique,
                             results=results)

    def _to_dataframe(self,
                      results: list,
                      original_tags: list) -> DataFrame:
        normalized = []

        s = set()
        s_unique = set()

        for tag in original_tags:
            for token in tag.split(" "):
                s.add(token)
        s = [x.strip().lower() for x in s if x]

        for result in results:

            _key = "{}-{}".format(result["ExplicitTag"], result["ImplicitTag"])
            if _key in s_unique:
                continue

            s_unique.add(_key)

            def _has_terms():
                for x in s:
                    if x in result["ExplicitTag"].lower():
                        return True
                    if x in result["ImplicitTag"].lower():
                        return True
                return False

            if not self.filter_on_key_terms \
                    or (self.filter_on_key_terms and _has_terms()):
                normalized.append(result)

        return pd.DataFrame(normalized)

    def process(self,
                some_tags: list) -> DataFrame:
        """
        :param some_tags:
            a list of tags to process (1..*)
        :return:
        """
        start = time.time()

        results = []
        s_unique = set()

        tags = [tag for tag in some_tags if tag and len(tag)]
        time_1 = time.time()

        for a_tag in tags:
            self._lookup(inference_counter=0,
                         an_existing_tag=a_tag,
                         original_tags=some_tags,
                         s_unique=s_unique,
                         results=results)
        time_2 = time.time()

        df_inference = self._to_dataframe(results=results,
                                          original_tags=some_tags)
        time_3 = time.time()

        if self._is_debug:
            self.logger.debug('\n'.join([
                "Inference Dataframe",
                tabulate(df_inference,
                         headers='keys',
                         tablefmt='psql')]))
        time_4 = time.time()

        if self._is_debug and not df_inference.empty:
            other_class = list(df_inference[df_inference['ExplicitSchema'] == 'other']['ExplicitTag'].unique())
            if len(other_class):
                self.logger.info('\n'.join([
                    "Schema 'Other' Classifications:",
                    f"\t{', '.join(other_class)}"]))
        else:
            self.logger.debug("Empty DataFrame")

        if self._is_debug:
            step_1 = round(time_1 - start, 2)
            step_2 = round(time_2 - time_1, 2)
            step_3 = round(time_3 - time_2, 2)
            step_4 = round(time_4 - time_3, 2)
            self.logger.debug(f"Generate Dataframe Rels "
                              f"(Step 1: {step_1}, "
                              f"Step 2: {step_2}, "
                              f"Step 3: {step_3}, "
                              f"Step 4: {step_4})")

        return df_inference
