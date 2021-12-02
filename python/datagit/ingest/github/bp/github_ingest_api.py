#!/usr/bin/env python
# -*- coding: UTF-8 -*-


import sys

from base import BaseObject


class GithubIngestAPI(BaseObject):
    """ GitHub Ingestion Orchestrator """

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
            *   update for n2n flow
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1476
        Updated:
            6-Dec-2019
            craig.trim@ibm.com
            *   add collection name generator
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1532
        Updated:
            7-Dec-2019
            craig.trim@ibm.com
            *   github-user-name no longer needed from environment
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1551#issuecomment-16473682
        """
        BaseObject.__init__(self, __name__)
        self._is_debug = is_debug
        self._github_usertoken = self._load_github_usertoken()

    def _load_github_usertoken(self):
        from datagit.ingest.core.dmo import GitHubTokenLoader

        return GitHubTokenLoader(is_debug=self._is_debug).user_tokens().github()

    def _target_collection_name(self,
                                repo_name: str,
                                collection_type: str) -> str:
        from datagit import CollectionNameGenerator
        return CollectionNameGenerator(is_debug=self._is_debug,
                                       repo_name=repo_name,
                                       collection_type=collection_type).process()

    def ingest(self):
        class Facade(object):

            @classmethod
            def by_name(cls,
                        repo_name: str,
                        repo_owner: str,
                        start_issue: int,
                        end_issue: int):
                from datagit.ingest.github.bp import GitHubIngestOrchestrator

                target_collection_name = self._target_collection_name(repo_name=repo_name,
                                                                      collection_type='src')

                GitHubIngestOrchestrator(flush_records=False,
                                         persist_records=True,
                                         is_debug=self._is_debug,
                                         repo_name=repo_name,
                                         repo_owner=repo_owner,
                                         github_usertoken=self._github_usertoken,
                                         collection_name=target_collection_name).process(start_issue=start_issue,
                                                                                         end_issue=end_issue)

            @classmethod
            def cendant(cls):
                return cls.by_name(repo_owner="GTS-CDO",
                                   repo_name="unstructured-analytics",
                                   start_issue=1,
                                   end_issue=sys.maxsize)

            @classmethod
            def cdo_workforce(cls):
                return cls.by_name(repo_owner="GTS-CDO",
                                   repo_name="CDO-Workforce-Transformation",
                                   start_issue=1,
                                   end_issue=sys.maxsize)

        return Facade()
