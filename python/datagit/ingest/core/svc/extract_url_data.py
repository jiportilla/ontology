#!/usr/bin/env python
# -*- coding: UTF-8 -*-


import json
import pprint
from typing import Optional

from requests import Response
from requests import Session

from base import BaseObject


class ExtractURLData(BaseObject):
    """ Given an active session, extract data from a URL """

    def __init__(self,
                 url: str,
                 session: Session,
                 is_debug: bool = False):
        """
        Created:
            29-Nov-2019
            craig.trim@ibm.com
            *   refactored out of 'persist-github-comments'
        Updated:
            2-Dec-2019
            craig.trim@ibm.com
            *   update to handle non-JSON data
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1492#issuecomment-16343096
        Updated:
            7-Dec-2019
            craig.trim@ibm.com
            *   refactored out of 'extract-github-data'
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1553#issue-11089485
        Updated:
            18-Dec-2019
            craig.trim@ibm.com
            *   try an alternate URL if the first one fails
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1626
        :param url:
        :param session:
        :param is_debug:
        """
        BaseObject.__init__(self, __name__)

        self._url = url
        self._session = session
        self._is_debug = is_debug

        if self._is_debug:
            self.logger.debug('\n'.join([
                "Instantiate ExtractURLData",
                f"\tURL: {url}"]))

    def _retry_url(self,
                   log_json_response: bool):
        """
        Purpose:
            -   For some reason, this alternative URL format can work
                when the original returns a 403
            -   Not certain why this is the case
        Reference
            https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1626
        """
        alt_url = f"https://api.github.ibm.com/repos/{self._url[35:]}"

        if self._is_debug:
            self.logger.debug('\n'.join([
                "Attempt Alternative URL",
                f"\tOriginal URL: {self._url}",
                f"\tAlternate URL: {alt_url}"]))

        return ExtractURLData(url=alt_url,
                              session=self._session,
                              is_debug=self._is_debug).process(log_json_response)

    def _decode(self,
                response: Response,
                log_json_response: bool) -> Optional[dict]:
        """ transform successful response to JSON dict """

        if log_json_response:
            self.logger.debug('\n'.join([
                "JSON Structure",
                pprint.pformat(response.json(), indent=4)]))

        try:
            return response.json()

        except json.decoder.JSONDecodeError as e:  # GIT-1492-16343096
            self.logger.exception(e)
            self.logger.error('\n'.join([
                "No JSON Content in URL",
                f"\tURL: {self._url}"]))

    def _log_error(self,
                   status_code: int) -> None:
        self.logger.error('\n'.join([
            "URL Retrieval Failure",
            f"\tURL: {self._url}",
            f"\tStatus Code: {status_code}"]))

    def process(self,
                log_json_response: bool = False) -> Optional[dict]:

        response = self._session.get(url=self._url)
        if self._is_debug:
            self.logger.info('\n'.join([
                "URL Retrieval Complete",
                f"\tStatus Code: {response.status_code}",
                f"\tURL: {self._url}"]))

        if response.status_code == 200:
            return self._decode(response, log_json_response)

        elif response.status_code == 403:

            # GIT-1626; try an alternative URL
            if "https://github.ibm.com/api/v3/repos" in self._url:
                return self._retry_url(log_json_response)

            self._log_error(response.status_code)
            return None

        elif response.status_code == 404:
            self._log_error(response.status_code)
            raise ValueError  # need to break here

        elif response.status_code == 410:
            # The 410 (Gone) status code means an old resource is permanently unavailable
            self._log_error(response.status_code)
            return None

        raise ValueError
