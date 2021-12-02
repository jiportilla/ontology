#!/usr/bin/env python
# -*- coding: UTF-8 -*-


import getpass
import os

from base import BaseObject
from base import CryptoBase


class Db2CredentialRetrieval(BaseObject):
    """ Set Input Credentials """

    def __init__(self,
                 db2_username_env: str,
                 db2_password_env: str):
        """
        Created:
            18-July-2019
            abhbasu3@in.ibm.com
        Updated:
            1-Aug-2019
            craig.trim@ibm.com
            *   retrieve username/password from the environment, if available
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/563
        Updated:
            3-August-2019
            abhbasu3@in.ibm.com
            *   added db2 env params to fetch appropriate username/password for different data sources
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/587
        Updated:
            9-Oct-2019
            craig.trim@ibm.com
            *   rename from 'db-credentials-api' and move to 'dmo' folder
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1080
        :param db2_username_env:
            the name of the env for db2 username
        :param db2_password_env:
            the name of the env for db2 password
        """
        BaseObject.__init__(self, __name__)

        self._db2_username_env = db2_username_env
        self._db2_password_env = db2_password_env

    def _from_env_vars(self):
        username = None
        password = None

        try:
            username = CryptoBase.decrypt_str(os.environ[self._db2_username_env])
            password = CryptoBase.decrypt_str(os.environ[self._db2_password_env])

        except KeyError as err:
            self.logger.warning('\n'.join([
                "Unable to retrieve Username and Password from the environment",
                "You will be prompted for this information"]))

        return username, password

    def _from_system_prompt(self):
        username = None
        password = None

        try:

            print("Enter credentials:\n")
            username = input("DB2 Username: ")
            password = getpass.getpass(prompt='DB2 Password: ')

        except Exception as err:
            self.logger.error(f"API Connection Error: {err}")

        return username, password

    def process(self):

        def credentials():
            u, p = self._from_env_vars()
            if u and p:
                return u, p

            u, p = self._from_system_prompt()
            if u and p:
                return u, p

            self.logger.error("DB2 LUR Credentials Inaccessible")

        username, password = credentials()
        if username:
            self.logger.info(f"API Parameters (db2-user = {username})")

        return username, password
