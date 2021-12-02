#!/usr/bin/env python
# -*- coding: UTF-8 -*-


import pandas as pd
from pandas import DataFrame

from base import BaseObject
from datadb2.core.dmo import BaseDB2Client


class SocialNetworkDataExtractor(BaseObject):
    """ Social NetWork Data Extractor """

    def __init__(self):
        """
        Created:
            - 13-July-2019
            - abhbasu3@in.ibm.com
        """
        BaseObject.__init__(self, __name__)

        # fetch credentials
        from datadb2.core.bp import DBCredentialsAPI
        self.db2username, self.db2password = DBCredentialsAPI("WFT_USER_NAME", "WFT_PASS_WORD").process()
        self.base_client = BaseDB2Client(some_username=self.db2username, some_password=self.db2password)

        self.schema_name = "WFTDEV"
        self.table_name = "SENTIMENT"
        self.base_client = BaseDB2Client(some_username=self.db2username, some_password=self.db2password)

    def load_table_n_columns(self, table_name, column_names, conn) -> DataFrame:
        s = ""
        for i in range(len(column_names) - 1):
            s += column_names[i]
            s += ", "
        s += column_names[-1]
        readquery = "select " + s + " from {}".format(table_name)
        df = pd.read_sql(readquery, conn)
        return df

    # CUSTOM QUERY WITH PARAMETERS
    def custom_query(self, query, conn):
        df = pd.read_sql(query, conn)
        return df

    def process(self) -> DataFrame:
        db2_data_df = self.load_table_n_columns('WFTDEV.SENTIMENT',
                                                ['AUTHOR', 'AUTHOR_EMAIL', 'CLASSIC_SENTIMENT', 'COMPOUND_SCORE',
                                                 'PARENT_URL', 'COMMENT_URL', 'CREATE_DATE', 'MODIFY_DATE', 'CONTENT'],
                                                self.base_client.connection)
        # close db connection
        self.base_client.close()

        db2_data_df.columns = [x.upper() for x in db2_data_df.columns]
        self.logger.debug("\n".join(["df headers: {}".format(list(db2_data_df.columns))]))
        db2_data_df = db2_data_df.dropna(subset=['AUTHOR_EMAIL'])

        return db2_data_df

    # Manager lookup
    def extract_manager_data(self) -> DataFrame:
        mgr_query = "SELECT E.LASTNAME AS EMP_LNAME,E.LEARNERCNUM,E.LEARNERINTRANETID AS EMAIL, M.LASTNAME AS MGR_LNAME, " \
                    "M.LEARNERCNUM AS MGR_CNUM, M.LEARNERINTRANETID AS MGR_EMAIL FROM (SELECT LASTNAME,LEARNERCNUM,LEARNERINTRANETID," \
                    "MANAGERCNUM FROM WFT.F_YL_LEARNER) E, (SELECT LASTNAME,LEARNERCNUM,LEARNERINTRANETID FROM WFT.F_YL_LEARNER) M " \
                    "WHERE E.MANAGERCNUM = M.LEARNERCNUM"

        df = self.custom_query(mgr_query, self.base_client.connection)

        # close db connection
        self.base_client.close()

        df.columns = [x.upper() for x in df.columns]

        return df

    # Functional Manager lookup
    def extract_functional_manager_data(self) -> DataFrame:
        func_mgr_query = "SELECT E.LEARNER_NAME AS EMP_NAME,E.LEARNERCNUM,E.LEARNERINTRANETID AS EMAIL, M.LEARNER_NAME AS MGR_NAME, " \
                         "M.LEARNERCNUM AS MGR_CNUM, M.LEARNERINTRANETID AS MGR_EMAIL FROM (SELECT LEARNER_NAME,LEARNERCNUM," \
                         "LEARNERINTRANETID,FUNCTIONALMANAGERCNUM FROM WFT.F_YL_LEARNER) E, (SELECT LEARNER_NAME,LEARNERCNUM," \
                         "LEARNERINTRANETID FROM WFT.F_YL_LEARNER) M WHERE E.FUNCTIONALMANAGERCNUM = M.LEARNERCNUM"

        df = self.custom_query(func_mgr_query, self.base_client.connection)

        # close db connection
        self.base_client.close()

        df.columns = [x.upper() for x in df.columns]

        return df
