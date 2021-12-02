#!/usr/bin/env python
# -*- coding: UTF-8 -*-


from pandas import DataFrame

from base import BaseObject


class InferenceComputer(BaseObject):
    """ Process Dimensionality for a Single Record only

    Sample Output:
        +----+----------------------+-------------------------------+----------------------+-----------------------+-------------+-------------+----------------+
        |    | ExplicitSchema       | ExplicitTag                   | ImplicitSchema       | ImplicitTag           | IsPrimary   | IsVariant   | Relationship   |
        |----+----------------------+-------------------------------+----------------------+-----------------------+-------------+-------------+----------------|
        |  0 | hard skill           | AWS Certification             | hard skill           | Certification         | True        | False       | Parent         |
        |  1 | hard skill           | Certification                 | soft skill           | Artifact              | False       | False       | Parent         |
        |  2 | soft skill           | Bank                          | soft skill           | Financial             | True        | False       | Parent         |
        ...
        | 54 | system administrator | Operating System              | hard skill           | Software              | False       | False       | Parent         |
        +----+----------------------+-------------------------------+----------------------+-----------------------+-------------+-------------+----------------+
     """

    def __init__(self,
                 xdm_schema: str,
                 df_evidence: DataFrame,
                 is_debug: bool = False):
        """
        Created:
            17-Oct-2019
            craig.trim@ibm.com
            *   refactored out of process-single-record
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
        :param df_evidence:
        :param is_debug
        """
        BaseObject.__init__(self, __name__)
        from nlusvc.core.bp import OntologyAPI

        self._is_debug = is_debug
        self._xdm_schema = xdm_schema
        self._df_evidence = df_evidence
        self._ontology_api = OntologyAPI(is_debug=self._is_debug)

    def process(self,
                add_tag_syns: bool = False,
                add_tag_rels: bool = True,
                add_rel_syns: bool = False,
                inference_level: int = 1,
                add_wiki_references: bool = False) -> DataFrame or None:
        """
        Purpose:
        :param add_tag_syns:
            -- refer to Ontology API documentation
        :param add_tag_rels:
            -- refer to Ontology API documentation
        :param add_rel_syns:
            -- refer to Ontology API documentation
        :param inference_level:
            -- refer to Ontology API documentation
        :param add_wiki_references:
            -- refer to Ontology API documentation
        :return:
            a DataFrame of results
        """
        tags = set()
        for _, row in self._df_evidence.iterrows():
            if "Tag" in row:
                tags.add(row["Tag"])

        tags = [x for x in tags if x and len(x)]
        if not tags or len(tags) == 0:
            self.logger.warning("No Tags Available")
            return None

        df_inference = self._ontology_api.inference(some_tags=sorted(tags),
                                                    xdm_schema=self._xdm_schema,
                                                    add_tag_syns=add_tag_syns,
                                                    add_tag_rels=add_tag_rels,
                                                    add_rel_syns=add_rel_syns,
                                                    inference_level=inference_level,
                                                    add_wiki_references=add_wiki_references,
                                                    is_debug=self._is_debug)

        if df_inference.empty:
            self.logger.warning("No Inference Computed")
            return None

        return df_inference
