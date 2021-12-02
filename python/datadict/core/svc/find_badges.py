# !/usr/bin/env python
# -*- coding: UTF-8 -*-

import os
from typing import Any, Dict, Union

from base import BaseObject

class FindBadges(BaseObject):
    """ a single API for badge analytics """

    _badges:Union[None, Dict] = None

    def __init__(self,
                 is_debug: bool = False):
        """
        Created:
            13-Oct-2019
            craig.trim@ibm.com
            *   https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1098#issuecomment-15265293
        Updated:
            15-Oct-2019
            craig.trim@ibm.com
            *   update 'tags-by-badge' function
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1124#issuecomment-15320883
        Updated:
            18-Oct-2019
            craig.trim@ibm.com
            *   add 'weight-deduction-by-badge'
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/886#issuecomment-15402867
        Updated:
            13-Jan-2020
            xavier.verges@es.ibm.com
            *   get the data from mongo rather than from MDA-generated dictionaries
        """
        BaseObject.__init__(self, __name__)

        self._is_debug = is_debug


    @classmethod
    def _load_badges_analysis(cls) -> None:
        from datamongo.core.bp import CendantCollection

        if 'BADGES_TAG_BUILD' in os.environ:
            collection_name = os.getenv('BADGES_TAG_BUILD')
        else:
            # TO-DO: do not use the hardcoded name. Use the collection registry
            collection_name = 'analytics_badges_tags'

        collection = CendantCollection(collection_name)
        as_records = collection.all()
        cls._badges = {}
        for record in as_records:
            cls._badges[record['badge'].lower()] = {
                'zScore': record['owners']['zScore'],
                'tags': record['tags']
            }

    @classmethod
    def _get_badges(cls) -> Dict:
        if not cls._badges:
            cls._load_badges_analysis()
        return cls._badges                      # type: ignore


    @classmethod
    def _get_badge_data(cls, badge_name):
        return cls._get_badges().get(badge_name.lower(), {'zScore': 0, 'tags': []})

    def weight_deduction_by_badge(self,
                                  badge_name: str,
                                  multiplier: float = 2.0) -> float:
        """
        Purpose:
            Return a Weight Deduction for a Badge
        Reference:
            https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/886#issuecomment-15402867
        :param badge_name:
            any badge name
        :param multiplier:
            (Optional)  increase the badge weight
        :return:
            a badge weight
        """

        zScore = self._get_badge_data(badge_name)['zScore']
        if zScore and zScore > 0:
            return zScore * multiplier
        return 0.0

    def tags_by_badge(self,
                      badge_name: str,
                      with_confidence_levels: bool = True) -> list:

        tags = self._get_badge_data(badge_name)['tags']
        if tags and len(tags) and not with_confidence_levels:
            tags = sorted([x[0] for x in tags])

        return tags

    def badge_by_tag(self,
                     tag_name):
        # conceptual method - might be interesting to analyze these results some day
        # but not necessary at present .... 13-Oct-2019
        raise NotImplementedError
