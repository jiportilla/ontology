# !/usr/bin/env python
# -*- coding: UTF-8 -*-


import pprint
from typing import Optional

from pandas import DataFrame
from tabulate import tabulate

from base import BaseObject
from datagit.navigate.dto import PatternStructure


class IssuePatternBuilder(BaseObject):
    """ Build a Collection of Recognized patterns for GitHub Issues

    GitHub Patterns:

        1.  Pull Request Pattern:
            An issue has multiple children of type 'mentions'
            each 'mention' has multiple children composed of:
                a.  one 'commit'
                b.  one 'merge'
                c.  multiple 'file-commit'
            this pattern represents a "Pull Request (PR)"
                where the PR was committed and merged
                and multiple files were modified

        2.  Issue Assignment
            An issue has a single child of type 'assignment'
            This represents the assignment of this issue to a team member

        3.  Issue Comment
            An issue has one or more children marked 'comment'
            These represent the comments made in the issue

    Sample Input:
        +-----+-----------------------------------------------------------------------------------------------+-----------------+-------------+
        |     | ID                                                                                            | Parent          | Type        |
        |-----+-----------------------------------------------------------------------------------------------+-----------------+-------------|
        |   0 | 1111                                                                                          | 1111            | issue       |
        |   1 | 1111-63467879                                                                                 | 1111            | assignment  |
        |   2 | 1111-63708424                                                                                 | 1111            | mentions    |
        |   3 | 1111-63708425                                                                                 | 1111            | mentions    |
        |   4 | 1111-66059720                                                                                 | 1111            | mentions    |
        |   5 | 1111-66059720-MDY6Q29tbWl0NTA4MzYzOjkzZGU4NjUwZWUwY2YzZThmNGY3YmEyYzc3OWY2ZTYxNTlkNzM3NGU=    | 1111-66059720   | commit      |
        ..
        | 185 | 1111-15919326                                                                                 | 1111            | comment     |
        | 186 | 1111-15921104                                                                                 | 1111            | comment     |
        | 187 | 1111-15947755                                                                                 | 1111            | comment     |
        +-----+-----------------------------------------------------------------------------------------------+-----------------+-------------+

    Sample Output:
        <reference dto/pattern_structure.py>

    Prereq:
        The taxonomy of GitHub Issues must exist
    """

    def __init__(self,
                 key_field: str,
                 d_index: dict,
                 df_taxonomy: DataFrame,
                 is_debug: bool = True):
        """
        Created:
            17-Dec-2019
            craig.trim@ibm.com
            *   https://github.ibm.com/-cdo/unstructured-analytics/issues/1543
        Updated:
            25-Dec-2019
            craig.trim@ibm.com
            *   remove hard stops in favor of continued processing
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1651#issuecomment-16807447
        :param is_debug:
            if True     increase log output at DEBUG level
        """
        BaseObject.__init__(self, __name__)

        self._d_index = d_index
        self._is_debug = is_debug
        self._key_field = key_field
        self._df_taxonomy = df_taxonomy

    @staticmethod
    def _retrieve_by_type(a_type: str,
                          a_df: DataFrame) -> Optional[list]:
        return sorted(a_df[a_df['Type'] == a_type]['ID'].unique())

    def _retrieve_commit_id(self,
                            df_children: DataFrame) -> Optional[str]:
        commits = self._retrieve_by_type('commit', df_children)
        if len(commits) == 1:
            return commits[0]  # Valid Outcome; default

        self.logger.warning('\n'.join([
            "Unrecognized Pattern (type=Commits)",
            f"\tActual (total={len(commits)}): {commits}",
            f"\tExpected = 1",
            f"{tabulate(df_children, headers='keys', tablefmt='psql')}"]))

    def _retrieve_merge(self,
                        commit_id) -> Optional[str]:
        merges = sorted(self._df_taxonomy[self._df_taxonomy['ID'] == commit_id]['ID'].unique())

        if len(merges) == 0:
            return None  # Valid Outcome; GIT-1543-16685587
        elif len(merges) == 1:
            return merges[0]  # Valid Outcome; default

        self.logger.warning('\n'.join([
            "Unrecognized Pattern (type=Merge)",
            f"\tActual (total={len(merges)}): {merges}",
            f"\tExpected = 1",
            f"\tCommit ID (Parent): {commit_id}"]))

    def _retrieve_file_commits(self,
                               df_children: DataFrame) -> Optional[list]:
        file_commits = self._retrieve_by_type('file-commit', df_children)
        if len(file_commits) > 0:
            return file_commits  # Valid Outcome; default

        self.logger.warning('\n'.join([
            "Unrecognized Pattern (type=File Commit)",
            f"\tActual (total={len(file_commits)}): {file_commits}",
            f"\tExpected > 0",
            f"{tabulate(df_children, headers='keys', tablefmt='psql')}"]))

    def _retrieve_pull_requests(self,
                                a_df: DataFrame) -> list:
        pull_requests = []
        mentions = self._retrieve_by_type('mentions', a_df)

        for mention in mentions:
            df_children = self._df_taxonomy[self._df_taxonomy['Parent'] == mention]

            if len(df_children) == 0:
                continue

            commit_id = self._retrieve_commit_id(df_children)
            if not commit_id:
                continue

            merge_id = self._retrieve_merge(commit_id)
            if not merge_id:
                continue

            file_commits = self._retrieve_file_commits(df_children)
            if not file_commits or not len(file_commits):
                continue

            pull_requests.append(
                PatternStructure.pull_request(pull_request_id=mention,
                                              commit_id=commit_id,
                                              merge_id=merge_id,
                                              file_commit_ids=file_commits))

        return pull_requests

    def _df_subset(self):
        return self._df_taxonomy[self._df_taxonomy['ID'].apply(
            lambda x: x.startswith(self._key_field))]

    def process(self) -> dict:
        df_subset = self._df_subset()

        comments = self._retrieve_by_type('comment', df_subset)
        assignments = self._retrieve_by_type('assignment', df_subset)
        pull_requests = self._retrieve_pull_requests(df_subset)

        pattern = PatternStructure.structure(comments=comments,
                                             assignments=assignments,
                                             issue_id=self._key_field,
                                             pull_requests=pull_requests)

        if self._is_debug:
            self.logger.debug('\n'.join([
                "Retrieved Issue Pattern",
                pprint.pformat(pattern, indent=4)]))

        return pattern
