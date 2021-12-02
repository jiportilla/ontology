#!/usr/bin/env python
# -*- coding: UTF-8 -*-


from base import BaseObject


class GitHubURLGenerator(BaseObject):
    """
        prepare url for Github repo API calls
        returns the url for coresponding repo owner & repo name
    """

    def __init__(self,
                 repo_name: str,
                 repo_owner: str,
                 is_debug: bool = False):
        """
        Created:
            20-Nov-2019
            craig.trim@ibm.com
            *   https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/574
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
        self._repo_name = repo_name
        self._repo_owner = repo_owner
        self._urls = GitHubTokenLoader(is_debug=self._is_debug).urls()

    def url(self):
        class Facade(object):

            @staticmethod
            def base():
                """
                Create a base GitHub URL
                :return:
                    a properly formatted URL
                """
                return f"{self._urls.github()}{self._repo_owner}/{self._repo_name}"

            @staticmethod
            def by_number(number: int):
                """
                Make url for Github repo issues
                :param number:
                :return:
                    a properly formatted issues URL for the specified repository
                """

                class ByNumberFacade(object):
                    @staticmethod
                    def issue():
                        return f"{self._urls.github()}{self._repo_owner}/{self._repo_name}/issues/{number}"

                    @staticmethod
                    def comments():
                        return f"{self._urls.github()}{self._repo_owner}/{self._repo_name}/issues/{number}/comments"

                return ByNumberFacade

        return Facade()
