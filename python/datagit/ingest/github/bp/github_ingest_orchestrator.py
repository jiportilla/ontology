#!/usr/bin/env python
# -*- coding: UTF-8 -*-


import sys

from requests import Session

from base import BaseObject
from base import DataTypeError
from datamongo import GitHubSrc


class GitHubIngestOrchestrator(BaseObject):
    """ Perform ETL (extract-transform-load) on GitHub data
    """

    def __init__(self,
                 repo_name: str,
                 repo_owner: str,
                 collection_name: str,
                 flush_records: bool = True,
                 persist_records: bool = True,
                 is_debug: bool = False):
        """
        Created:
            29-Nov-2019
            craig.trim@ibm.com
        Updated:
            3-Dec-2019
            craig.trim@ibm.com
            *   add additional filtering and update controller pattern
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1509#issuecomment-16374938
        Updated:
            7-Dec-2019
            craig.trim@ibm.com
            *   remove github-user-name from param list
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1551#issuecomment-16473682
            *   renamed from 'perform-github-etl'
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1553#issue-11089485
        Updated:
            2-Jan-2020
            craig.trim@ibm.com
            *   remove github-usertoken as a param
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1692
        """
        BaseObject.__init__(self, __name__)

        from datagit.ingest.github.svc import PerformGitHubETL

        self._is_debug = is_debug
        self._repo_name = repo_name
        self._repo_owner = repo_owner
        self._flush_records = flush_records
        self._persist_records = persist_records

        self._etl = PerformGitHubETL(is_debug=self._is_debug,
                                     repo_name=self._repo_name,
                                     repo_owner=self._repo_owner)

        self._github_collection = GitHubSrc(is_debug=self._is_debug,
                                            collection_name=collection_name)

    def _open_session(self) -> Session:
        from datagit import GitHubSessionAuthenticator
        from datagit.ingest.core.dmo import GitHubTokenLoader

        github_usertoken = GitHubTokenLoader(is_debug=self._is_debug).user_tokens().github()
        return GitHubSessionAuthenticator(github_usertoken).session()

    def _generate_url(self,
                      issue_id: int) -> str:
        """
        Purpose:
            Generate the appropriate GitHub URL
        :param issue_id:
            any issue ID number
        :return:
            a properly formatted URL string
        """
        from datagit import GitHubURLGenerator

        github_url = GitHubURLGenerator(is_debug=self._is_debug,
                                        repo_name=self._repo_name,
                                        repo_owner=self._repo_owner).url().by_number(issue_id).issue()

        if self._is_debug:
            self.logger.debug('\n'.join([
                "Generation Complete",
                f"\tURL: {github_url}"]))

        return github_url

    @staticmethod
    def _start_issue(start_issue: int) -> int:
        if start_issue == 0:
            return 1
        return start_issue

    @staticmethod
    def _end_issue(end_issue: int):
        if type(end_issue) != int:
            raise DataTypeError("End Issue, int")
        if end_issue:
            return end_issue + 1
        return sys.maxsize

    def process(self,
                start_issue: int = 0,
                end_issue: int = None,
                bulk_load_threshold: int = 100) -> None:
        """
        :param start_issue:
            the start issue to work with
        :param end_issue:
            the last issue to work with
        :param bulk_load_threshold:
        """

        master_records = []
        session = self._open_session()

        if self._flush_records:
            self._github_collection.flush()

        x = self._start_issue(start_issue)
        y = self._end_issue(end_issue)

        for issue_id in range(x, y):
            if self._is_debug:
                self.logger.debug(f"Processing Issue: {issue_id}, {x}-{y}")

            try:
                github_url = self._generate_url(issue_id=issue_id)
                d_records = self._etl.process(issue_id=issue_id,
                                              parent_id=issue_id,
                                              session=session,
                                              github_url=github_url,
                                              d_records={})
                if not d_records:
                    continue

                for url in d_records:
                    for record in d_records[url]:
                        master_records.append(record)

            except ValueError as e:  # this is not the nicest way to stop a process
                # but we have no way of knowing how many issues exist in a GitHub Repo
                # so we have to wait for a 404 error from a non-existent issue number
                self.logger.error(f"Processing Stopped")
                self.logger.exception(e)
                break

            if not master_records or not len(master_records):
                self.logger.warning("No Records To Load")
                return

            if self._persist_records and len(master_records) >= bulk_load_threshold:
                self._github_collection.insert(master_records)
                master_records = []

        if self._persist_records and len(master_records):
            self._github_collection.insert(master_records)
