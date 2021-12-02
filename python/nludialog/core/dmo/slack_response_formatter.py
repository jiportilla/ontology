#!/usr/bin/env python
# -*- coding: UTF-8 -*-


from base import BaseObject


class SlackResponseFormatter(BaseObject):
    """ Format a Response for a Slack Channel """

    def __init__(self,
                 is_debug: bool = False):
        """
        Created:
            18-Jul-2019
            craig.trim@ibm.com
            *   https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/453
        """
        BaseObject.__init__(self, __name__)
        self.is_debug = is_debug

    def process(self,
                user: str,
                input_text: str) -> str:
        response = f"<@{user}> {input_text}"

        if self.is_debug:
            self.logger.debug("\n".join([
                "Generated Response",
                "\t{}".format(response)]))

        return response
