import os
import re

import pandas as pd
from pandas import DataFrame

from base import BaseObject
from base import MandatoryParamError


class IngestDataExtractorDB(BaseObject):
    """ ingest data from DB2 """

    def __init__(self,
                 some_activity: str,
                 some_sql_query: str,
                 some_db2_username: str,
                 some_db2_password: str,
                 db2_hostname: str,
                 db2_dbname: str,
                 db2_port: str):
        """
        Created:
            26-June-2019
            abhbasu3@in.ibm.com
            *   db2 data extract
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/370
        Updated:
            25-July-2019
            abhbasu3@in.ibm.com
            *   db2 details added as param
        :param some_activity:
            the name of the ingestion activity
        :param some_sql_query:
            some_sql_query
        :param some_db2_username:
            db2 username
        :param some_db2_password:
            db2 password
        :param db2_hostname:
            db2 hostname
        :param db2_dbname:
            db2 dbname
        :param db2_port:
            db2 port
        """
        BaseObject.__init__(self, __name__)
        if not some_activity:
            raise MandatoryParamError("Activity")
        if not some_sql_query:
            raise MandatoryParamError("SQL Query")
        if not some_db2_username:
            raise MandatoryParamError("DB2 Username")
        if not some_db2_password:
            raise MandatoryParamError("DB2 Password")
        if not db2_hostname:
            raise MandatoryParamError("DB2 Hostname")
        if not db2_dbname:
            raise MandatoryParamError("DB2 DB-name")
        if not db2_port:
            raise MandatoryParamError("DB2 Port")

        self.activity = some_activity
        self.sql_query = some_sql_query
        self.db2username = some_db2_username
        self.db2password = some_db2_password
        self.db2_hostname = db2_hostname
        self.db2_dbname = db2_dbname
        self.db2_port = db2_port

        self.logger.debug("\n".join([
            "DB2 details:",
            "\tdb2_hostname: {}".format(self.db2_hostname),
            "\tdb2_dbname: {}".format(self.db2_dbname),
            "\tdb2_port: {}".format(self.db2_port)]))

    def _debug_db2(self, df: DataFrame) -> None:
        if 'KEEP_DB2_QUERIES_AS_EXCEL' in os.environ:
            file_name = re.sub(r'\s', '-', self.sql_query)
            file_name = re.sub(r'(?u)[^-\w.]', '', file_name)
            path = os.path.join(os.environ["CODE_BASE"], "resources/output", file_name + '.xlsx')
            writer = pd.ExcelWriter(path)
            df.to_excel(writer)
            writer.close()

    def process(self) -> DataFrame:
        from datadb2.core.dmo import BaseDB2Client

        # Connect to db2
        base_client = BaseDB2Client(some_database_name=self.db2_dbname,
                                    some_hostname=self.db2_hostname,
                                    some_port=int(self.db2_port),
                                    some_username=self.db2username,
                                    some_password=self.db2password)

        # fetch dataset from db2 table as dataframe
        df = pd.read_sql_query(self.sql_query, con=base_client.connection)
        # close db2 connection
        base_client.close()
        self._debug_db2(df)

        if df.empty is True:
            raise Exception("\n".join(["Dataset not found"]))

        # change the column headers to uppercase to match manifest files source names
        df.columns = [x.upper() for x in df.columns]

        return df
