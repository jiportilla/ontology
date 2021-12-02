# !/usr/bin/env python
# -*- coding: UTF-8 -*-


from tabulate import tabulate

from base import BaseObject
from cendantdim import ProcessSingleRecord
from datadict import FindDimensions
from datamongo import CendantCollectionCategory


class RunRegressionSuite(BaseObject):
    __schemas = {}

    def __init__(self,
                 input_file: str,
                 is_debug: bool = False):
        """
        Created:
            20-Aug-2019
            craig.trim@ibm.com
            *   https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/786
        Updated:
            29-Oct-2019
            craig.trim@ibm.com
            *   remove 'entity-schema-finder' in favor of new approach
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/916#issuecomment-15620801
        :param input_file:
        :param is_debug:
        """
        BaseObject.__init__(self, __name__)
        self._is_debug = is_debug
        self._input_file = input_file

    @staticmethod
    def _schema_lookup(xdm_schema: str,
                       some_value: str) -> list:
        """
        :param xdm_schema:
            the name of the schema to perform the type lookup
            Notes:
            -   typically either 'supply' or 'learning'
            -   the full list is on path 'resources/config/dimensionality'
        :param some_value:
        :return:
        """
        return FindDimensions(xdm_schema).find(some_value)

    def by_serial_answer(self,
                         collection_name_tag: str,
                         collection_name_xdm: str,
                         serial_number: str = None):
        from testsuite.dimensions.dmo import MockRecordGenerator
        from testsuite.dimensions.dmo import RegressionInputTransformer

        df_regression = RegressionInputTransformer(
            input_file=self._input_file,
            serial_number=serial_number,
            is_debug=self._is_debug).dataframe()

        records = MockRecordGenerator(
            df_regression=df_regression,
            is_debug=self._is_debug).records()

        process_single_record = ProcessSingleRecord(d_manifest={},
                                                    xdm_schema='supply',
                                                    collection_name_tag=collection_name_tag,
                                                    collection_name_xdm=collection_name_xdm,
                                                    is_debug=self._is_debug)

        for record in records:
            df_zscores, df_schema_weights = process_single_record.compute(record)

            self.logger.debug('\n'.join([
                f"Dimension Schema Weights (serial-number={serial_number})",
                tabulate(df_zscores,
                         headers='keys',
                         tablefmt='psql')]))
