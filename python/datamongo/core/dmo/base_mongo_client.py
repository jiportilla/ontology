#!/usr/bin/env python
# -*- coding: UTF-8 -*-


import os
import warnings

from pymongo import MongoClient

from base import BaseObject
from base import CredentialsFromJson
from base import CryptoBase


class BaseMongoClient(BaseObject):
    """ Create a MongoDB connection """

    def __init__(self,
                 some_mongo_host: str = None,
                 server_alias: str = None,
                 is_debug: bool = False):
        """
        Created:
            14-Apr-2019
            craig.trim@ibm.com
            *   based on 'base-mongo-client-1'
        Updated:
            6-Jun-2019
            craig.trim@ibm.com
            *   enable username/password authentication
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/336
        Updated:
            15-Aug-2019
            craig.trim@ibm.com
            *   added 'log' function
        Updated:
            04-Dec-2019
            xavier.verges@es.ibm.com
            *   moved code to CredentialsFromJson
        Updated:
            15-Jan-2020
            xavier.verges@es.ibm.com
            *   added server_alias param
        Updated:
            13-Feb-2020
            craig.trim@ibm.com
            *   default instantiation to 'cloud' if no parameter provided
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1855
        Updated:
            23-Feb-2020
            xavier.verges@es.ibm.com
            *   Ignore server alias when running on kubernetes. This is a quick hack
                because the skills api is breaking in kuberentes.
                Proper solution would be to remove server alias from everywhere and
                migrate our servers to the cloud instance.
        """
        BaseObject.__init__(self, __name__)
        if some_mongo_host:
            warning_txt = 'the some_mongo_host parameter is ignored; set MONGO_JSON_CREDENTIALS instead'
            # Do not use DeprecationWarning because it is hidden by jupyter and REPL
            warnings.warn(warning_txt, Warning)

        # if not some_mongo_host and not server_alias:
        #     server_alias = 'CLOUD'  # GIT-1855-17765027

        if 'KUBERNETES_SERVICE_HOST' in os.environ:
            server_alias = None

        if server_alias:
            server_alias = server_alias.upper()
            if server_alias not in ['WFTAG', 'CLOUD']:
                raise ValueError('Invalid server alias')

        url = None
        ca_file = None
        if server_alias or 'MONGO_JSON_CREDENTIALS' in os.environ:
            env_var_name = f'MONGO_JSON_CREDENTIALS_{server_alias}' if server_alias else 'MONGO_JSON_CREDENTIALS'
            credentials = CredentialsFromJson(os.environ[env_var_name],
                                              'mongodb')
            url = credentials.url
            ca_file = credentials.ca_file
        if not url:
            host = os.environ["MONGO_HOST"]
            port = int(os.environ["MONGO_PORT"])
            username = CryptoBase.decrypt_str(os.environ["MONGO_USER_NAME"])
            password = CryptoBase.decrypt_str(os.environ["MONGO_PASS_WORD"])
            url = f"mongodb://{username}:{password}@{host}:{port}/"
        if not url:
            raise RuntimeError("Bad MONGO_ environment variables")
        self.client = MongoClient(url, ssl_ca_certs=ca_file)
        self.url = CredentialsFromJson.sanitize_url(url, 'mongodb')

        if is_debug:
            self.logger.debug('\n'.join([
                "MongoDB Connection Opened",
                f"\tURL: {self.url}"]))

    def log(self) -> str:
        return f"Connected on {self.url}"
