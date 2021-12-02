#!/usr/bin/env python
# -*- coding: UTF-8 -*-


from base import BaseObject


class GitHubURLFilter(BaseObject):
    """ Remove URLs that have already been accessed

    Challenges:
        The same URL can have two different formats

        Sample URL-1:
            https://api.github.ibm.com/repos/GTS-CDO/unstructured-analytics/issues/1510

        Sample URL-2:
            https://github.ibm.com/api/v3/repos/GTS-CDO/unstructured-analytics/issues/1510

        The above URLs are identical in content, but not in URL construction

        the 'cleanse(...)' routine will compare like-for-like by only comparing the portion of the string
            aft4er the '/repos/' path:

            Sample Cleansed URL-1:
                GTS-CDO/unstructured-analytics/issues/1510

            Sample Cleansed URL-2:
                GTS-CDO/unstructured-analytics/issues/1510

        and this makes it clear that both URLs are identical
    """

    def __init__(self,
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
        Created:
            7-Dec-2019
            craig.trim@ibm.com
            *   refactored out of 'github-ingest-orchestrator'
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1553#issue-11089485
        """
        BaseObject.__init__(self, __name__)
        self._is_debug = is_debug

    def process(self,
                d_records: dict,
                candidate_urls: list):
        """
        :param d_records:
            a dictionary of GitHub results keyed by URL
        :param candidate_urls:
             a list of URL strings for Cendant to access
             this list may include URLs that have already been accessed
        :return:
            a list of URL strings that Cendant has never accessed before
        """

        def cleanse(a_url: str) -> str:
            return a_url.split('/repos/')[-1].strip()

        known_urls = [cleanse(url)
                      for url in sorted(d_records.keys())]

        unknown_urls = [url for url in candidate_urls
                        if url and cleanse(url) not in known_urls]

        if self._is_debug and len(unknown_urls):
            self.logger.debug('\n'.join([
                "Located Unknown URLs",
                f"\tURLs: {unknown_urls}"]))

        return unknown_urls
