#!/usr/bin/env python
# -*- coding: UTF-8 -*-


import os.path
import time

import pandas as pd
from pandas import DataFrame

from base import BaseObject
from base import MandatoryParamError


class IngestDataExtractor(BaseObject):
    """ ingest data from a provenance manifest """

    def __init__(self,
                 some_manifest: dict):
        """
        Created:
            11-Mar-2019
            craig.trim@ibm.com
        :param some_manifest:
            the name of the ingestion activity
        """
        BaseObject.__init__(self, __name__)
        if not some_manifest:
            raise MandatoryParamError("Manifest")

        self.manifest = some_manifest

    @staticmethod
    def _field_to_columns(l_fields: list) -> dict:
        """
        transform the field definitions section of the manifest to a
        pandas compatible columns dictionary
        :param l_fields:
            multiple fields of this format:
                  - target_name: first_name
                    source_name: FRST_NM
                    data_type: str
                  - target_name: last_name
                    source_name: LST_NM
                    data_type: str
        :return:
            a dictionary that looks like this
                {   "FRST_NM": str,
                    "LST_NM": str }
        """

        def _data_type(a_field):
            if a_field["data_type"] == "int":
                return "int"
            elif a_field["data_type"] == "float":
                return "float"
            # elif a_field["data_type"] == "date":
            #     return "date"
            return "str"

        cols = {}
        for field in l_fields:
            cols[field["source_name"]] = _data_type(field)

        return cols

    @staticmethod
    def _fields_to_position(l_fields: list) -> list:
        """
        transform the field definitions section of the manifest to a
        list of positions for CSV importing
        :param l_fields:
            multiple fields of this format:
                  - target_name: first_name
                    source_name: FRST_NM
                    data_type: str
                    position: 4
                  - target_name: last_name
                    source_name: LST_NM
                    data_type: str
                    position: 8
        :return:
            a list that looks like this
                [ 4, 8 ]
        """
        return [x["position"] for x in l_fields if "position" in x]

    def _excel_source_as_df(self,
                            path: str,
                            cols: dict,
                            skiprows=0,
                            position=None,
                            sheet_name=None) -> DataFrame:

        from dataingest.core.dmo import ExcelReader

        start = time.time()

        def _sheet_name():
            """ if no sheet name supplied in params
                take the first one from the spreadsheet """
            if sheet_name:
                return sheet_name
            return ExcelReader.sheet_names(path)[0]

        sheet_name = _sheet_name()
        df = ExcelReader.read_excel(some_input_path=path,
                                    some_sheet_name=sheet_name,
                                    skiprows=skiprows,
                                    usecols=position,
                                    column_names=cols)

        if len(df) == 0:
            raise ValueError("\n".join([
                "No Records Loaded",
                "\tinput-path: {}".format(path),
                "\tsheet-name: {}".format(sheet_name),
            ]))

        self.logger.debug("\n".join([
            "Loaded Source Excel File as DataFrame",
            "\tinput-path: {}".format(path),
            "\tsheet-name: {}".format(sheet_name),
            "\ttotal-records: {}".format(len(df)),
            "\ttotal-time: ~{}s".format(int(time.time() - start))
        ]))

        return df

    def _csv_source_as_df(self,
                          path: str,
                          cols: dict,
                          positions: list,
                          delim: str,
                          skiprows=0,
                          sheet_name=None) -> DataFrame:

        start = time.time()

        # Remind analyst to double check use of positioning parameter
        # in the ingest manifest
        self.logger.info("\n".join([
            "Adivsory: Manifest Column Positioning is 0-Based",
            "\tPositions: {}".format(positions)]))

        df = pd.read_csv(path,
                         engine="python",
                         delim_whitespace=False,
                         sep=delim,
                         error_bad_lines=False,
                         warn_bad_lines=True,
                         parse_dates=True,
                         infer_datetime_format=True,
                         skip_blank_lines=True,
                         skiprows=skiprows,
                         comment='#',
                         encoding='utf-8',
                         names=list(cols.keys()),
                         dtype=cols,
                         na_values=['none', 'None'],
                         usecols=positions)
        #
        # df = pd.read_csv(path,
        #                  engine="python",
        #                  sep=delim,
        #                  skiprows=skiprows,
        #                  names=list(cols.keys()),
        #                  usecols=positions)

        if len(df) == 0:
            raise ValueError("\n".join([
                "No Records Loaded (path={}, name={})".format(
                    path, sheet_name)]))

        df.fillna(value='', inplace=True)

        self.logger.debug("\n".join([
            "Loaded Source CSV File as DataFrame",
            "\tInput Path: {}".format(path),
            "\tSheet Name: {}".format(sheet_name),
            "\tTotal Records: {}".format(len(df)),
            "\tTotal Time: ~{}s".format(int(time.time() - start))]))

        return df

    def process(self) -> DataFrame:

        d_source = self.manifest["source"]
        d_fields = self.manifest["fields"]
        skiprows = d_source["skiprows"]

        d_source["path"] = os.path.expandvars(d_source["path"])
        if not os.path.isfile(d_source["path"]):
            raise ValueError("\n".join([
                "File Not Found",
                "\tInput Path: {}".format(d_source["path"])]))

        if d_source["type"].lower() == "excel":
            field_position = ",".join(self._fields_to_position(d_fields))
            if len(field_position) == 0:
                field_position = None

            if field_position:
                self.logger.debug("\n".join([
                    "Field Positions",
                    "\t{}".format(field_position)]))

            return self._excel_source_as_df(path=d_source["path"],
                                            position=field_position,
                                            cols=self._field_to_columns(d_fields),
                                            skiprows=skiprows)

        elif d_source["type"].lower() == "csv":
            return self._csv_source_as_df(path=d_source["path"],
                                          cols=self._field_to_columns(d_fields),
                                          positions=self._fields_to_position(d_fields),
                                          delim=d_source["delim"],
                                          skiprows=skiprows)

        raise NotImplementedError("\n".join([
            "Source Type Not Implemented",
            "\tType: {}".format(d_source["type"])]))
