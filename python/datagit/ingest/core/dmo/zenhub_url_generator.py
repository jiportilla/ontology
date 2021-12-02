#!/usr/bin/env python
# -*- coding: UTF-8 -*-


from base import BaseObject


class ZenHubURLGenerator(BaseObject):
    """
        prepare url for ZenHub repo API calls
        returns the url for coresponding repo owner & repo name
    """

    def __init__(self,
                 repo_id: int,
                 repo_name: str,
                 repo_owner: str,
                 is_debug: bool = False):
        """
        Created:
            7-Dec-2019
            craig.trim@ibm.com
            *   generated via
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1511
            *   refactored in pursuit of
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1553#issue-11089485
        :param repo_id:
            the numeric identifier of the GitHub repository
        :param repo_owner:
            name of the Github repo owner
            e.g.,   'GTS-CDO'
        :param repo_name:
            name of the Github repo
            e.g.,   'unstructured-analytics'
        """
        BaseObject.__init__(self, __name__)
        from datagit.ingest.core.dmo import GitHubTokenLoader
        self._is_debug = is_debug

        self._repo_id = repo_id
        self._repo_id = repo_id
        self._repo_name = repo_name
        self._repo_owner = repo_owner

        self._token_loader = GitHubTokenLoader(is_debug=self._is_debug)

    def url(self):
        class Facade(object):

            @staticmethod
            def base():
                """
                Create a base ZenHub URL
                :return:
                    a properly formatted URL
                """
                return self._token_loader.urls().zenhub()

            @staticmethod
            def issue(number: int):
                """
                Make url for ZenHub repo issues
                :param number:
                :return:
                    a properly formatted issues URL for the specified repository
                """
                token = self._token_loader.user_tokens().zenhub()
                return f"{Facade().base()}{self._repo_id}/issues/{number}?{token}"

            @staticmethod
            def epic(number: int):
                """
                Make url for ZenHub epic issues
                Reference:
                    https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1667#issuecomment-16821516
                :param number:
                :return:
                    a properly formatted issues URL for the specified repository
                """
                token = self._token_loader.user_tokens().zenhub()
                return f"{Facade().base()}{self._repo_id}/epics/{number}?{token}"

            @staticmethod
            def pull(number: int):
                """
                Make url for ZenHub repo pull requests
                :param number:
                :return:
                    a properly formatted pull URL for the specified repository
                """
                token = self._token_loader.user_tokens().zenhub()
                return f"{Facade().base()}{self._repo_id}/pull/{number}?{token}"

        return Facade()
