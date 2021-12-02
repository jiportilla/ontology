#!/usr/bin/env python
# -*- coding: UTF-8 -*-


import requests
from requests import Session

from base import BaseObject


class GitHubSessionAuthenticator(BaseObject):
    """ Authenticate to Github and open a Session """

    def __init__(self,
                 github_usertoken: str,
                 is_debug: bool = False):
        """
        Created:
            26-Nov-2019
            craig.trim@ibm.com
            *   https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1459
        Updated:
            7-Dec-2019
            craig.trim@ibm.com
            *   user-name appears to be an unecessary param for authentication
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1551#issuecomment-16473682
            *   renamed from 'github-session-loader'
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1553#issue-11089485
        :param github_usertoken:
        :param is_debug:
        """
        BaseObject.__init__(self, __name__)

        self._is_debug = is_debug
        self._github_usertoken = github_usertoken

        self._session = requests.Session()
        self._authenticate()

    def _authenticate(self) -> None:
        __NO_USERNAME_REQUIRED = ""  # GIT-1551-16473682
        self._session.auth = (__NO_USERNAME_REQUIRED,
                              self._github_usertoken)
        self._session.verify = True

    def session(self) -> Session:
        return self._session
