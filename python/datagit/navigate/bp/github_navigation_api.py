# !/usr/bin/env python
# -*- coding: UTF-8 -*-


from typing import Optional

from base import BaseObject
from base import DataTypeError


class GitHubNavigationAPI(BaseObject):
    """ API for Record Navigation """

    __d_github_navigator = {}

    def __init__(self,
                 is_debug: bool = True):
        """
        Created:
            17-Dec-2019
            craig.trim@ibm.com
            *   https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1542
        Updated:
            2-Jan-2020
            craig.trim@ibm.com
            *   renamed from 'github-analysis-api'
                https://github.ibm.com/-cdo/unstructured-analytics/issues/1680#issuecomment-16901240
        :param is_debug:
            if True     increase log output at DEBUG level
        """
        BaseObject.__init__(self, __name__)

        self._is_debug = is_debug

    def navigate(self,
                 collection_name: str):
        from datagit.navigate.svc import NavigateGitHubRecords

        if collection_name not in self.__d_github_navigator:
            self.__d_github_navigator[collection_name] = NavigateGitHubRecords(
                is_debug=self._is_debug,
                collection_name=collection_name)

        class Facade(object):

            def __init__(self,
                         a_navigator: NavigateGitHubRecords):
                self._navigator = a_navigator

            def by_issue(self,
                         issue_id: str or int) -> Optional[dict]:
                def _issue_id() -> str:
                    if type(issue_id) == int:
                        return str(issue_id)
                    if type(issue_id) == str:
                        return issue_id
                    raise DataTypeError

                return self._navigator.process(issue_id=_issue_id())

            @staticmethod
            def all():
                pass

        return Facade(a_navigator=self.__d_github_navigator[collection_name])
