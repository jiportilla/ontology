#!/usr/bin/env python
# -*- coding: UTF-8 -*-


from typing import Optional

from requests import Session

from base import BaseObject


class FindRepoID(BaseObject):
    """ Every GitHub Repository has an underlying numerical ID
        This service will retrieve that ID """

    def __init__(self,
                 repo_owner: str,
                 repo_name: str,
                 is_debug: bool = False):
        """
        Created:
            7-Dec-2019
            craig.trim@ibm.com
            *   https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1551
        Updated:
            18-Dec-2019
            craig.trim@ibm.com
            *   use github-token-loader rather than retrieve github-usertoken from environment
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1626#issuecomment-16729731
        :param repo_owner:
            name of the Github repo owner
            e.g.,   'GTS-CDO'
        :param repo_name:
            name of the Github repo
            e.g.,   'unstructured-analytics'
        :param is_debug:
        """
        BaseObject.__init__(self, __name__)
        self._is_debug = is_debug
        self._repo_name = repo_name
        self._repo_owner = repo_owner

    def _base_url(self):
        from datagit.ingest.core.dmo import GitHubURLGenerator

        return GitHubURLGenerator(is_debug=True,
                                  repo_owner=self._repo_owner,
                                  repo_name=self._repo_name).url().base()

    def _open_session(self) -> Session:
        from datagit.ingest.core.dmo import GitHubSessionAuthenticator
        from datagit.ingest.core.dmo import GitHubTokenLoader
        self._github_usertoken = GitHubTokenLoader(is_debug=self._is_debug).user_tokens().github()

        return GitHubSessionAuthenticator(is_debug=self._is_debug,
                                          github_usertoken=self._github_usertoken).session()

    def process(self) -> Optional[int]:
        session = self._open_session()
        _response = session.get(self._base_url())

        if _response.status_code != 200:
            self.logger.error('\n'.join([
                "GitHub Authentication Failure",
                f"\tStatus Code: {_response.status_code}"]))
            raise ValueError

        repo_id = int(_response.json()["id"])

        if self._is_debug:
            self.logger.debug('\n'.join([
                f"Located Repository ID",
                f"\tRepo ID: {repo_id}",
                f"\tRepo Owner: {self._repo_owner}",
                f"\tRepo Name: {self._repo_name}"]))

        return repo_id
