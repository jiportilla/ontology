#!/usr/bin/env python
# -*- coding: UTF-8 -*-


import pprint
from typing import Optional

from base import BaseObject
from datagit.ingest.core.dmo import ZenHubURLGenerator


class AccessZenHubURL(BaseObject):
    """ Perform ZenHub Access
    """

    def __init__(self,
                 repo_name: str,
                 repo_owner: str,
                 is_debug: bool = False):
        """
        Created:
            7-Dec-2019
            craig.trim@ibm.com
            *   https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1511
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

        self._token_loader = GitHubTokenLoader(is_debug=self._is_debug)

    def _repo_id(self) -> int:
        from datagit.ingest.core.svc import FindRepoID
        return FindRepoID(is_debug=self._is_debug,
                          repo_owner=self._repo_owner,
                          repo_name=self._repo_name).process()

    def _issue_url(self,
                   repo_id: int,
                   issue_id: int) -> str:
        return ZenHubURLGenerator(is_debug=self._is_debug,
                                  repo_id=repo_id,
                                  repo_owner=self._repo_owner,
                                  repo_name=self._repo_name).url().issue(number=issue_id)

    def _epic_url(self,
                  repo_id: int,
                  issue_id: int) -> str:
        return ZenHubURLGenerator(is_debug=self._is_debug,
                                  repo_id=repo_id,
                                  repo_owner=self._repo_owner,
                                  repo_name=self._repo_name).url().epic(number=issue_id)

    def process(self,
                issue_id: int,
                as_epic: bool = False) -> Optional[dict]:
        from datagit.ingest.core.dmo import GitHubSessionAuthenticator

        repo_id = self._repo_id()

        def generate_url() -> str:
            if as_epic:
                return self._epic_url(repo_id=repo_id,
                                      issue_id=issue_id)
            return self._issue_url(repo_id=repo_id,
                                   issue_id=issue_id)

        url = generate_url()

        github_usertoken = self._token_loader.user_tokens().github()
        session = GitHubSessionAuthenticator(is_debug=self._is_debug,
                                             github_usertoken=github_usertoken).session()

        zenhub_usertoken = self._token_loader.user_tokens().zenhub()
        headers = {'X-Authentication-Token': zenhub_usertoken}

        response = session.get(url,
                               headers=headers,
                               verify=False)

        if response.status_code != 200:
            self.logger.error('\n'.join([
                "URL Retrieval Failure",
                f"\tIssue ID: {issue_id}",
                f"\tStatus Code: {response.status_code}"]))
            return None

        if self._is_debug:
            self.logger.debug('\n'.join([
                "ZenHub Content Retrieved",
                f"\tURL: {url}",
                f"\tIssue ID: {issue_id}",
                pprint.pformat(response.json())]))

        return response.json()
