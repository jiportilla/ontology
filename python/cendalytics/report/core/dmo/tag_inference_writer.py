#!/usr/bin/env python
# -*- coding: utf-8 -*-


from openpyxl.worksheet.worksheet import Worksheet
from pandas import DataFrame

from base import BaseObject
from cendantdim import InferenceComputer
from nlusvc import EvidenceExtractor


class TagInferenceWriter(BaseObject):

    def __init__(self,
                 excel_worksheet: Worksheet,
                 tag_record: dict,
                 xdm_schema: str,
                 is_debug: bool = False):
        """
        Created:
            15-Nov-2019
            craig.trim@ibm.com
            *   https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1331#issuecomment-16027118
        :param excel_worksheet:
            the excel worksheet to write to
        :param tag_record:
        :param xdm_schema:
            the name of the schema to perform the type lookup
            Notes:
            -   typically either 'supply' or 'learning'
            -   the full list is on path 'resources/config/dimensionality'
        """
        BaseObject.__init__(self, __name__)
        from . import WorksheetHelper

        self._is_debug = is_debug
        self._tag_record = tag_record
        self._xdm_schema = xdm_schema
        self._excel_worksheet = excel_worksheet

        self._helper = WorksheetHelper

    def _inference(self,
                   add_tag_syns: bool,
                   add_tag_rels: bool,
                   add_rel_syns: bool,
                   inference_level: int,
                   add_wiki_references: bool):

        # Step: and Compute the Dimemsionality Dataframe
        df_evidence = EvidenceExtractor(some_records=[self._tag_record],
                                        xdm_schema='supply',
                                        is_debug=self._is_debug).process()
        if df_evidence is None:
            self.logger.error("Evidence Extraction Failure")
            return None

        # Step: Infer additional tags
        inference = InferenceComputer(is_debug=self._is_debug,
                                      df_evidence=df_evidence,
                                      xdm_schema=self._xdm_schema)

        return inference.process(add_tag_syns=add_tag_syns,
                                 add_tag_rels=add_tag_rels,
                                 add_rel_syns=add_rel_syns,
                                 inference_level=inference_level,
                                 add_wiki_references=add_wiki_references)

    def _define_header(self) -> dict:
        d_cols = {
            'A': 20,
            'B': 20,
            'C': 20,
            'D': 20,
            'E': 20,
            'F': 20,
            'G': 20}

        self._helper.column_widths(self._excel_worksheet, d_cols)

        def _header_other(value: str) -> dict:
            return self._helper.struct(value, "header_other")

        return {
            'A1': _header_other("Explicit Schema"),
            'B1': _header_other("Explicit Tag"),
            'C1': _header_other("Implicit Schema"),
            'D1': _header_other("Implicit Tag"),
            'E1': _header_other("Is Primary"),
            'F1': _header_other("Is Variant"),
            'G1': _header_other("Relationship")}

    def _define_rows(self,
                     df_inference: DataFrame) -> list:
        rows = []

        def _collection_value(value: str or int) -> dict:
            return self._helper.struct(value, "collection_value")

        ctr = 2
        for _, row in df_inference.iterrows():
            def _is_primary() -> int:
                return int(bool(row["IsPrimary"]))

            def _is_variant() -> int:
                return int(bool(row["IsVariant"]))

            rows.append({
                f'A{ctr}': _collection_value(row["ExplicitSchema"]),
                f'B{ctr}': _collection_value(row["ExplicitTag"]),
                f'C{ctr}': _collection_value(row["ImplicitSchema"]),
                f'D{ctr}': _collection_value(row["ImplicitTag"]),
                f'E{ctr}': _collection_value(_is_primary()),
                f'F{ctr}': _collection_value(_is_variant()),
                f'G{ctr}': _collection_value(row["Relationship"])})

            ctr += 1

        return rows

    def process(self,
                add_tag_syns: bool,
                add_tag_rels: bool,
                add_rel_syns: bool,
                inference_level: int,
                add_wiki_references: bool) -> None:

        df_inference = self._inference(add_tag_syns=add_tag_syns,
                                       add_tag_rels=add_tag_rels,
                                       add_rel_syns=add_rel_syns,
                                       inference_level=inference_level,
                                       add_wiki_references=add_wiki_references)

        if df_inference is None or df_inference.empty:
            self.logger.warning('\n'.join([
                "Inference Computation Failed",
                f"\tKey Field: {self._tag_record['key_field']}"]))
            return None

        rows = [self._define_header()]

        [rows.append(x) for x in self._define_rows(df_inference)]

        self._helper.generate(l_structs=rows,
                              worksheet=self._excel_worksheet)
