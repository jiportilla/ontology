#!/usr/bin/env python
# -*- coding: UTF-8 -*-


import os

from base import BaseObject
from base import CryptoBase
from base import FileIO
from datadb2.core.dmo import BaseDB2Client


class BuildDb2Url(BaseObject):
    """ Create a DB2 connection """

    __config_path = 'resources/config/db2/schemas.yml'

    def __init__(self,
                 is_debug: bool = False):
        """
        Created:
            9-Oct-2019
            craig.trim@ibm.com
            *   https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1080
        :param is_debug:
        """
        BaseObject.__init__(self, __name__)
        self._is_debug = is_debug
        self._config = FileIO.file_to_yaml_by_relative_path(self.__config_path)

    @staticmethod
    def _values(d_config: dict):
        username = CryptoBase.decrypt_str(os.environ[d_config['username'][1:]])
        password = CryptoBase.decrypt_str(os.environ[d_config['password'][1:]])

        return {
            'host': d_config['host'].strip(),
            'database': d_config['database'].strip(),
            'port': d_config['port'],
            'username': username.strip(),
            'password': password.strip()}

    @staticmethod
    def _connect(d_config: dict) -> BaseDB2Client:
        return BaseDB2Client(some_database_name=d_config['database'],
                             some_hostname=d_config['host'],
                             some_port=d_config['port'],
                             some_username=d_config['username'],
                             some_password=d_config['password'])

    def wft_dev(self) -> BaseDB2Client:
        """
        Purpose:
            Connect to DB2 WFT DEV
        :return:
        """
        return self._connect(self._values(self._config['wft_dev']))

    def cendant(self) -> BaseDB2Client:
        """

        :return:
        """
        return self._connect(self._values(self._config['cendant']))


if __name__ == "__main__":
    # BuildDb2Url().wft_dev()
    # BuildDb2Url().wft_prod()
    BuildDb2Url().cendant()
