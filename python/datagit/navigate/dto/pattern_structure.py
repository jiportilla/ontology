# !/usr/bin/env python
# -*- coding: UTF-8 -*-


from typing import Optional

from base import DataTypeError
from base import MandatoryParamError


class PatternStructure(object):
    """ Data Structure for a single GitHub Pattern

    Sample Structure
        [   {   'issue': '1111',
                'comments': [ '1111-15347705',
                              '1111-15347724',
                              ...
                              '1111-15947755'],
                'pull-requests': [
                    {   'commit': '1111-66059720-MDY6Q29tbWl0NTA4MzYzOjkzZGU4NjUwZWUwY2YzZThmNGY3YmEyYzc3OWY2ZTYxNTlkNzM3NGU=',
                         'files': [   '1111-66059720-MDY6Q29tbWl0NTA4MzYzOjkzZGU4NjUwZWUwY2YzZThmNGY3YmEyYzc3OWY2ZTYxNTlkNzM3NGU=-1',
                                      '1111-66059720-MDY6Q29tbWl0NTA4MzYzOjkzZGU4NjUwZWUwY2YzZThmNGY3YmEyYzc3OWY2ZTYxNTlkNzM3NGU=-10',
                                      ...
                                      '1111-66059720-MDY6Q29tbWl0NTA4MzYzOjkzZGU4NjUwZWUwY2YzZThmNGY3YmEyYzc3OWY2ZTYxNTlkNzM3NGU=-9'],
                         'id': '1111-66059720',
                         'merge': '1111-66059720-MDY6Q29tbWl0NTA4MzYzOjkzZGU4NjUwZWUwY2YzZThmNGY3YmEyYzc3OWY2ZTYxNTlkNzM3NGU='},
                    ...
                     {   'commit': '1111-66735318-MDY6Q29tbWl0NTA4MzYzOjAxODMyZmY3MTYzNDY3NDUyY2JlYWQzNTAyNTM4NDhhYTAwMWU4ZGM=',
                         'files': [   '1111-66735318-MDY6Q29tbWl0NTA4MzYzOjAxODMyZmY3MTYzNDY3NDUyY2JlYWQzNTAyNTM4NDhhYTAwMWU4ZGM=-1',
                                      '1111-66735318-MDY6Q29tbWl0NTA4MzYzOjAxODMyZmY3MTYzNDY3NDUyY2JlYWQzNTAyNTM4NDhhYTAwMWU4ZGM=-10',
                                      ...
                                      '1111-66735318-MDY6Q29tbWl0NTA4MzYzOjAxODMyZmY3MTYzNDY3NDUyY2JlYWQzNTAyNTM4NDhhYTAwMWU4ZGM=-9'],
                         'id': '1111-66735318',
                         'merge': '1111-66735318-MDY6Q29tbWl0NTA4MzYzOjAxODMyZmY3MTYzNDY3NDUyY2JlYWQzNTAyNTM4NDhhYTAwMWU4ZGM='}]}]
    """

    @staticmethod
    def structure(issue_id: str,
                  assignments: Optional[list],
                  comments: Optional[list],
                  pull_requests: Optional[list]) -> dict:

        if not issue_id:
            raise MandatoryParamError

        if comments is None:
            comments = []  # comments are optional
        if type(comments) != list:
            raise DataTypeError

        if assignments is None:
            assignments = []  # assignments are optional
        if type(assignments) != list:
            raise DataTypeError

        if pull_requests is None:
            pull_requests = []  # pull requests are optional
        if type(pull_requests) != list:
            raise DataTypeError

        return {
            "issue": issue_id,
            "comments": comments,
            "assignments": assignments,
            "pull-requests": pull_requests,
            "provenance": {"activity": "pattern-builder"}}

    @staticmethod
    def pull_request(pull_request_id: str,
                     commit_id: str,
                     merge_id: Optional[str],
                     file_commit_ids: Optional[list]) -> dict:

        if not pull_request_id:
            raise MandatoryParamError
        if type(pull_request_id) != str:
            raise DataTypeError

        if not commit_id:
            raise MandatoryParamError
        if type(commit_id) != str:
            raise DataTypeError

        if merge_id and type(merge_id) != str:
            raise DataTypeError  # merge_id is optional

        if file_commit_ids is None:
            file_commit_ids = []
        if type(file_commit_ids) != list:
            raise DataTypeError

        return {
            "id": pull_request_id,
            "commit": commit_id,
            "merge": merge_id,
            "files": file_commit_ids}
