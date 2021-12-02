#!/usr/bin/env python
# -*- coding: UTF-8 -*-


import os
from random import randint

from base import BaseObject
from base import CryptoBase


class GitHubTokenLoader(BaseObject):
    """
    """

    __github_usertokens = [
        "GITHUB_USER_TOKEN_1",
        "GITHUB_USER_TOKEN_2",
        "GITHUB_USER_TOKEN_3",
        "GITHUB_USER_TOKEN_4",
        "GITHUB_USER_TOKEN_5",
        "GITHUB_USER_TOKEN_6",
        "GITHUB_USER_TOKEN_7",
        "GITHUB_USER_TOKEN_8",
        "GITHUB_USER_TOKEN_9",
        "GITHUB_USER_TOKEN_10",
        "GITHUB_USER_TOKEN_11",
        "GITHUB_USER_TOKEN_12",
    ]  # GIT-1626-16729731

    def __init__(self,
                 is_debug: bool = False):
        """
        Created:
            20-Nov-2019
            craig.trim@ibm.com
            *   https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/574
        Updated:
            29-Nov-2019
            craig.trim@ibm.com
        Updated:
            7-Dec-2019
            craig.trim@ibm.com
            *   remove github-user-name
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1551#issuecomment-16473682
        Updated:
            18-Dec-2019
            craig.trim@ibm.com
            *   add multiple user tokens
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1626#issuecomment-16729731
        """
        BaseObject.__init__(self, __name__)
        self._is_debug = is_debug
        self._github_user_token = self._user_token()
        self._github_base_url = self._envar_decrypt("GITHUB_BASE_URL")
        self._zenhub_base_url = self._envar_decrypt("ZENHUB_BASE_URL")
        self._zenhub_user_token = self._envar_decrypt("ZENHUB_USER_TOKEN")

    def _user_token(self):
        def envar() -> str:  # GIT-1626-16729731
            return self.__github_usertokens[
                randint(0, len(self.__github_usertokens) - 1)]

        envar = envar()
        if self._is_debug:
            self.logger.debug('\n'.join([
                "Retrieved Random User Token",
                f"\tName: {envar}"]))

        return self._envar_decrypt(envar)

    def _envar_decrypt(self,
                       name: str) -> str:
        return CryptoBase.decrypt_str(self._envar(name=name))

    def _envar(self,
               name: str) -> str:
        try:
            return os.environ[name]
        except KeyError:
            self.logger.error(f"Environment Variable Not Found: "
                              f"(name={name})")

    def urls(self):
        class Facade(object):
            @staticmethod
            def github() -> str:
                return self._github_base_url

            @staticmethod
            def zenhub() -> str:
                return self._zenhub_base_url

        return Facade()

    def user_tokens(self):
        class Facade(object):

            @staticmethod
            def github() -> str:
                return self._github_user_token

            @staticmethod
            def zenhub() -> str:
                return self._zenhub_user_token

        return Facade()
