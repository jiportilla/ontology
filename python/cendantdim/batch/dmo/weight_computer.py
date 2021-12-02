#!/usr/bin/env python
# -*- coding: UTF-8 -*-


from pandas import DataFrame
from tabulate import tabulate

from base import BaseObject
from datadict import FindDimensions


class WeightComputer(BaseObject):
    """ Purpose: Build a DataFrame with Weights

    Sample Output:
        +----+----------------------+----------+
        |    | Schema               |   Weight |
        |----+----------------------+----------|
        |  0 | blockchain           |    0     |
        |  1 | quantum              |    0     |
        |  2 | cloud                |    0.237 |
        |  3 | system administrator |    2.715 |
        |  4 | database             |    0     |
        |  5 | data science         |    0     |
        |  6 | hard skill           |    4.751 |
        |  7 | soft skill           |    3.927 |
        |  8 | project management   |    0.237 |
        |  9 | service management   |    4.467 |
        | 10 | other                |    3.021 |
        | 11 | learning             |    0     |
        +----+----------------------+----------+
    """

    def __init__(self,
                 key_field: str,
                 df_evidence: DataFrame,
                 df_inference: DataFrame,
                 xdm_schema: str,
                 is_debug: bool = False):
        """
        Created:
            17-Oct-2019
            craig.trim@ibm.com
            *   refactored out of process-single-record
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1142#issuecomment-15371983
        Updated:
            29-Oct-2019
            craig.trim@ibm.com
            *   remove 'entity-schema-finder' in favor of new approach
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/916#issuecomment-15620801
        :param key_field:
        :param df_evidence:
        :param df_inference:
        :param xdm_schema:
            the name of the schema to perform the type lookup
            Notes:
            -   typically either 'supply' or 'learning'
            -   the full list is on path 'resources/config/dimensionality'
        :param is_debug
        """
        BaseObject.__init__(self, __name__)
        from nlusvc.core.bp import OntologyAPI

        self._is_debug = is_debug
        self._key_field = key_field
        self._xdm_schema = xdm_schema
        self._df_evidence = df_evidence
        self._df_inference = df_inference
        self._dim_finder = FindDimensions(xdm_schema)
        self._ontology_api = OntologyAPI(is_debug=is_debug)

    def process(self) -> DataFrame or None:
        df_schema_weights = self._ontology_api.weight(df_evidence=self._df_evidence,
                                                      df_inference=self._df_inference,
                                                      xdm_schema=self._xdm_schema,
                                                      add_collection_weight=True,
                                                      add_field_name_weight=True,
                                                      add_badge_distribution_weight=True,
                                                      add_implicit_weights=True)

        if self._is_debug:
            self.logger.info("\n".join([
                f"Key Field: {self._key_field}",
                "\n{}".format(tabulate(df_schema_weights,
                                       headers='keys',
                                       tablefmt='psql'))]))

        if df_schema_weights.empty:
            self.logger.warning("No Schema Weighting Performed")
            return None

        return df_schema_weights
