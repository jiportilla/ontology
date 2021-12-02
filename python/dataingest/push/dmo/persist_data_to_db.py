#!/usr/bin/env python
# -*- coding: UTF-8 -*-


import inspect

import pandas as pd

from base import BaseObject
from base import MandatoryParamError
from datadb2 import BaseDB2Client


class PersistDatatoDB(BaseObject):
    """ persist data to db2

        Updated:
            23-Feb-2020
            xavier.verges@es.ibm.com
            *   Extracted individual steps to its own methods so the class can be used
                to persist collections incrementally and without recreating the connection
            *   Added __enter__ and __exit__ methods so that we can do
                with PersistDatatoDB() as xx
                and be sure that the connection is closed no matter how we leave
    """

    def __init__(self,
                 username: str,
                 password: str):
        """
        Created:
            04-July-2019
            abhbasu3@in.ibm.com
        Updated:
            2-Aug-2019
            craig.trim@ibm.com
            *   refactored
        Updated:
            30-September-2019
            abhbasu3@in.ibm.com
            * updated `is_truncate` parameter. Default set to `True`. `TRUNCATE` command runs by default
            https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1018
        Updated:
            7-November-2019
            abhbasu3@in.ibm.com
            * added tag & xdm query
            * persist tag & xdm mongo collection to DB2
            https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1239
        :param username:
            the db2 username
        :param password:
            the db2 passowrd
        """
        BaseObject.__init__(self, __name__)
        if not username:
            raise MandatoryParamError("DB2 Username")
        if not password:
            raise MandatoryParamError("DB2 Password")

        self._db2 = self._connect(username, password)
        self._connected = True
        self.logger.debug(f"Opened DB2 Connection")

    @staticmethod
    def _connect(username: str,
                 password: str) -> BaseDB2Client:
        return BaseDB2Client(some_username=username,
                             some_password=password)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close_connection()

    def __del__(self):
        self.close_connection()

    def close_connection(self):
        if self._connected:
            self._db2.close()
            self.logger.debug(f"Closed DB2 Connection")
            self._connected = False

    def create_table(self,
                     schema_name: str,
                     table_name: str,
                     transformation_type: str) -> None:

        if transformation_type.lower() not in ["tag", "xdm", "fields"]:
            raise Exception(f"Collection type '{transformation_type}' not supported")

        schema_name = schema_name.upper()
        table_name = table_name.upper()

        ddl = {
            "tag": f"""CREATE TABLE {schema_name}.{table_name}(
                    CONFIDENCE DECIMAL(14,5),
                    FIELDID VARCHAR(500),
                    KEYFIELD VARCHAR(500),
                    TAG VARCHAR(500));""",
            "xdm": f"""CREATE TABLE {schema_name}.{table_name}(
                        KEYFIELD VARCHAR(500),
                        SLOT VARCHAR(500),
                        SLOTWEIGHT DECIMAL(14,5),
                        SLOTZSCORE DECIMAL(14,5),
                        COLLECTION VARCHAR(500));""",
            "fields": f"""CREATE TABLE {schema_name}.{table_name}(
                        COLLECTION VARCHAR(500),
                        FIELDID VARCHAR(500),
                        FIELDNAME VARCHAR(500),
                        KEYFIELD VARCHAR(500),
                        NORMALIZEDTEXT CLOB,
                        ORIGINALTEXT CLOB,
                        PRIORCOLLECTION VARCHAR(500));""",
        }
        command = ddl[transformation_type.lower()]
        command = inspect.cleandoc(command)
        command = " ".join(command.splitlines())
        self.logger.info(f"DB2 command for {transformation_type} table: {command}")
        self._db2.connection .execute(command)

    def clear_table(self,
                    schema_name: str,
                    table_name: str) -> None:
        schema_name = schema_name.upper()
        table_name = table_name.upper()
        command = f"TRUNCATE TABLE {schema_name}.{table_name} IMMEDIATE;"
        self.logger.info(f"DB2 command: {command}")
        self._db2.connection.execute(command)

    def table_exists(self,
                     schema_name: str,
                     table_name: str) -> bool:
        schema_name = schema_name.upper()
        table_name = table_name.upper()
        return self._db2.connection.dialect.has_table(self._db2.connection, table_name, schema=schema_name)

    def insert_dataframe_into_table(self,
                                    df: pd.DataFrame,
                                    schema_name: str,
                                    table_name: str) -> None:
        schema_name = schema_name.upper()
        table_name = table_name.upper()
        self.logger.debug(f"DB2 inserting dataframe of shape {df.shape} into table {schema_name}.{table_name}")
        df.to_sql(table_name,
                  self._db2.connection,
                  schema_name,
                  if_exists='append',
                  index=False)

    def update_refresh_stats(self,
                             schema_name: str,
                             table_name: str) -> None:
        schema_name = schema_name.upper()
        table_name = table_name.upper()
        command = ''
        if table_name == "D_SELF_IDENTIFIED_CERTIFICATIONS" and schema_name == "WFT":
            command = """
                      UPDATE  WFT.DATASOURCE_REFRESH_STATS SET WFT_DATASOURCEUPDTS = CURRENT TIMESTAMP
                      WHERE WFT_TABLENAME = 'D_SELF_IDENTIFIED_CERTIFICATIONS' WITH UR"""

        if command:
            command = inspect.cleandoc(command)
            self._db2.connection.execute(command)

    def process(self,
                input_data,
                schema_name: str,
                table_name: str,
                is_truncate: bool = True,
                transformation_type: str = None) -> None:

        input_df = None

        # check if the input is dataframe
        if isinstance(input_data, pd.DataFrame):
            input_df = input_data

        # get input dataframe if input is csv
        else:
            if input_data.endswith(".csv"):
                input_df = pd.read_csv(input, header=0)

        if input_df is None:
            raise Exception("Input Data format not supported")

        if transformation_type in ["tag", "xdm", "fields"]:
            self.create_table(schema_name, table_name, transformation_type)

        # truncate table if `is_truncate` set to `True`
        if is_truncate:
            self.clear_table(schema_name, table_name)

        # check if table exists
        if not self.table_exists(schema_name, table_name):
            raise Exception(f"DB2 Table {table_name} not found")

        # insert data in db2 table
        self.insert_dataframe_into_table(input_df, schema_name, table_name)

        # update data refresh stats
        self.update_refresh_stats(schema_name, table_name)

        # clean up
        self.close_connection()

        self.logger.info("Data inserted in DB2!")
