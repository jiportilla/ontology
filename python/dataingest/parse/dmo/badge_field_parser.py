#!/usr/bin/env python
# -*- coding: UTF-8 -*-


from base import BaseObject
from datadict import FindBadges


class BadgeFieldParser(BaseObject):
    """  Parse a field classified as 'badge'
    """

    def __init__(self,
                 is_debug: bool = False):
        """
        Created:
            16-Aug-2019
            craig.trim@ibm.com
            *   refactored out of 'parse-records-from-mongo' in pursuit of
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/768
        Updated:
            22-Aug-2019
            craig.trim@ibm.com
            *   return tag-tuples for badge tags
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/796
        Updated:
            13-Oct-2019
            craig.trim@ibm.com
            *   replace mongoDB collection lookup for badges with optimized dictionary lookup
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1098#issuecomment-15265293
        """
        BaseObject.__init__(self, __name__)
        self._is_debug = is_debug
        self._find_badges = FindBadges()

        if self._is_debug:
            self.logger.debug("Instantiate BadgeFieldParser")

    def process(self,
                badge_field: dict):
        """
        :param badge_field:
        :return:
            an augmented badge field
        """

        badge_label = badge_field["value"][0]
        tags = self._find_badges.tags_by_badge(badge_label)

        def _tags():
            return {
                "supervised": tags,
                "unsupervised": []}

        badge_field["tags"] = _tags()
        badge_field["normalized"] = badge_field["value"]

        return badge_field
