#!/usr/bin/env python
# -*- coding: UTF-8 -*-


import os
import socket
import time

from base import BaseObject


class BaseDB2Client(BaseObject):
    """ Create a DB2 connection """

    def __init__(self,
                 some_username: str,
                 some_password: str,
                 some_port: int = 50000,
                 some_hostname: str = 'dpydalwftdb01.sl.bluecloud.ibm.com',
                 some_database_name: str = 'wftdev'):
        """
        Created:
            26-June-2019
            abhbasu3@in.ibm.com
            *   db2 connection
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/370
        Updated:
            9-Oct-2019
            craig.trim@ibm.com
            *   update strict typing and remove unnecessary state and update logging
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1080
        Updated:
            10-Feb-2020
            xavier.verges@es.ibm.com
            *   Try to check with sockets if the host is reachable
        :param some_database_name:
            name of the DB2 database
        :param some_hostname:
            hostname to connect to db2
        :param some_port:
            port to connect to db2
        :param some_username:
            username for db2 authentication
        :param some_password:
            password for db2 authentication
        """
        BaseObject.__init__(self, __name__)
        self._port = some_port
        self._host = some_hostname
        self._db_name = some_database_name

        self._db2_url = f"{some_username}:{some_password}@{self._host}:{self._port}/{self._db_name}"

        # actual connection
        self.connection = self.open_connection()

    def _is_reachable(self):
        reachable = False
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            socketTimeout = 5
            s.settimeout(socketTimeout)
            s.connect((self._host, self._port))
            s.close()
            reachable = True
            return
        except:                 # noqa: E722
            pass
        msg = 'Can' if reachable else 'Cannot'
        self.logger.info(f'{msg} reach {self._host}:{self._port} using sockets')
        if not reachable:
            os.system(f'tracepath -p {self._port} {self._host}')
        return reachable

    def open_connection(self):

        # keep the imports at method level
        # so that environments without ibm_db installed won't fail
        from sqlalchemy import create_engine
        from sqlalchemy import exc

        # dbtype = 'DB2'
        retry_sleep_interval_seconds = 5
        max_retries = 4
        retry_count = 0

        while retry_count <= max_retries:
            try:
                # self._is_reachable()
                self.logger.debug(f'Attempting connection to {self._host}:{self._port}/{self._db_name} ...')
                dbengine = create_engine("db2+ibm_db://" + self._db2_url)

                dbcreateconnection = dbengine.connect()
                self.logger.info('\n'.join([
                    "DB2 Connection Successful",
                    f"\tHost: {self._host}:{self._port}",
                    f"\tDatabase Name: {self._db_name}"]))

                return dbcreateconnection

            except exc.SQLAlchemyError as err:
                if retry_count != max_retries:
                    retry_count += 1
                    retry_sleep_interval_seconds = 2 * retry_sleep_interval_seconds
                    self.logger.info(f"Connection to {self._host}:{self._port}/{self._db_name} "
                                     f"failed, attempt number: {retry_count} "
                                     f"Sleeping for {retry_sleep_interval_seconds} seconds and trying again")
                    self.logger.info(f"error message: {err}")
                    time.sleep(retry_sleep_interval_seconds)
                else:
                    self.logger.info("Unable to connect to DB2 database. Script exiting, Please try again.")
                    raise err

    def close(self):
        self.connection.close()


if __name__ == "__main__":
    from base import CryptoBase

    u = CryptoBase.decrypt_str(os.environ['WFT_USER_NAME'])
    p = CryptoBase.decrypt_str(os.environ['WFT_PASS_WORD'])
    BaseDB2Client(some_username=u, some_password=p)
