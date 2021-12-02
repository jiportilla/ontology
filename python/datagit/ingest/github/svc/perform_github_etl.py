#!/usr/bin/env python
# -*- coding: UTF-8 -*-


from requests import Session

from base import BaseObject


class PerformGitHubETL(BaseObject):
    """ Extract, Transform and Load (ETL) GitHub Data """

    def __init__(self,
                 repo_name: str,
                 repo_owner: str,
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
            *   integrate zenhub access
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1511
        """
        BaseObject.__init__(self, __name__)
        from datagit.ingest.core.dmo import GitHubURLFinder
        from datagit.ingest.core.dmo import GitHubURLFilter

        self._is_debug = is_debug
        self._repo_name = repo_name
        self._repo_owner = repo_owner

        self._url_finder = GitHubURLFinder(is_debug=False)
        self._url_filter = GitHubURLFilter(is_debug=False)

    def _transform(self,
                   issue_id: int,
                   parent_id: str,
                   github_url: str,
                   url_result: list or dict) -> list:
        from datagit.ingest.github.svc import TransformGitHubStructure

        return TransformGitHubStructure(issue_id=issue_id,
                                        parent_id=parent_id,
                                        svcresult=url_result,
                                        is_debug=self._is_debug,
                                        manifest_name=github_url,
                                        repo_name=self._repo_name,
                                        repo_owner=self._repo_owner).process()

    def _extract(self,
                 issue_id: int,
                 session: Session,
                 github_url: str) -> list or dict or None:
        from datagit import ExtractURLData

        url_result = ExtractURLData(url=github_url,
                                    session=session,
                                    is_debug=self._is_debug).process(log_json_response=False)

        if not url_result or not len(url_result):
            self.logger.warning('\n'.join([
                "No Data Extracted",
                f"\tIssue #{issue_id}",
                f"\tURL: {github_url}"]))
            return []

        if self._is_debug:
            self.logger.debug('\n'.join([
                "URL Data Extraction Complete",
                f"\tKey Sizes: {len(url_result)}",
                f"\tURL: {github_url}"]))

        return url_result

    def process(self,
                issue_id: int,
                parent_id: str or None,
                session: Session,
                github_url: str,
                d_records: dict) -> list or None:

        url_result = self._extract(session=session,
                                   issue_id=issue_id,
                                   github_url=github_url)
        if not url_result:
            return []

        source_records = self._transform(issue_id=issue_id,
                                         parent_id=parent_id,
                                         github_url=github_url,
                                         url_result=url_result)

        if not source_records or not len(source_records):
            return d_records

        d_records[github_url] = source_records

        for source_record in d_records[github_url]:
            urls = self._url_finder.process(source_record)
            urls = self._url_filter.process(d_records=d_records,
                                            candidate_urls=urls)

            for url in urls:
                def _parent_id():
                    if source_record['key_field']:
                        return source_record['key_field']
                    return issue_id

                d_inner_records = self.process(issue_id=issue_id,
                                               parent_id=_parent_id(),
                                               session=session,
                                               github_url=url,
                                               d_records=d_records)

                if not d_inner_records or not len(d_inner_records):
                    continue

                for k in d_inner_records:
                    values = [value for value in d_inner_records[k]
                              if value and type(value) == dict]
                    for value in values:
                        if k in d_records:
                            continue
                        if k not in d_records:
                            d_records[k] = []
                        d_records[k].append(value)

        return d_records
