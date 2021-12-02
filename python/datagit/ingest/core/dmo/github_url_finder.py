#!/usr/bin/env python
# -*- coding: UTF-8 -*-


from base import BaseObject


class GitHubURLFinder(BaseObject):
    """ Retrieve Fields with valid API URLs

    Data Structure:
        a cendant record has 1..* fields
    Sample Input:
        a URL field looks something like this
            [{   'agent': 'system',
                'collection': 'Comments',
                'field_id': '52320b7a-165d-11ea-ad01-acde48001122',
                'name': 'IssueURL',
                'transformations': [],
                'type': 'url',
                'value': 'https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1510#issuecomment-16375456'}]
    """

    def __init__(self,
                 is_debug: bool = False):
        """
        Created:
            7-Dec-2019
            craig.trim@ibm.com
            *   refactored out of 'github-ingest-orchestrator'
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1553#issue-11089485
        """
        BaseObject.__init__(self, __name__)

        self._is_debug = is_debug

    def process(self,
                source_record: dict) -> list:
        """
        :param source_record:
            a cendant record containing multiple fields
        :return:
            a list of URLs (strings) of valid API URLs
        """
        fields = [field for field in source_record["fields"]  # retrieve all fields with URLs
                  if field["type"] == "url"]

        fields = [field for field in fields  # filter out raw URLs
                  if field["name"] != "RawURL"]  # GIT-1492-16343157

        fields = [field for field in fields  # filter out HTML URLs
                  if field["name"] != "HtmlURL"]  # GIT-1509-16374938

        urls = [field["value"] for field in fields]

        if self._is_debug:
            self.logger.debug('\n'.join([
                "API URL Filtering Complete",
                f"\tFinal URLs: {urls}"]))

        return urls
