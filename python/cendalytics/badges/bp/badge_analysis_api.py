#!/usr/bin/env python
# -*- coding: UTF-8 -*-

import time

from base import BaseObject

IS_DEBUG = True

class BadgeAnalysisAPI(BaseObject):
    """ API for Manifest based badge analysis activities """

    def __init__(self,
                 manifest_name: str,
                 activity_name: str,
                 first: int=-1,
                 last: int=-1):
        """
        Created:
            10-Jan-2020
            xavier.verges@es.ibm.com
        """
        from cendalytics.badges.svc import BadgeAnalysisManifestData
        BaseObject.__init__(self, __name__)
        self.analyzer = BadgeAnalysisManifestData(manifest_name,
                                                  activity_name,
                                                  first,
                                                  last,
                                                  is_debug=IS_DEBUG)

    def analyze_per_badge(self):
        self.analyzer.process()

    def analyze_distribution(self):
        self.analyzer.analyze_distribution()

    def flush_target(self):
        return self.analyzer.flush_target()

    def get_sources(self):
        return self.analyzer.source_collections()


def call_badge_analysis_api(manifest_name, activity_name, action, first, last):
    first = int(first) if first else -1
    last = int(last) if last else -1
    print(f"API Parameters "
          f"(manifest-name={manifest_name}, "
          f"activity-name={activity_name}, "
          f"action={action}, "
          f"first={first}, last={last})")

    if manifest_name.startswith("badge-analysis"):
        api = BadgeAnalysisAPI(manifest_name,
                               activity_name,
                               first,
                               last)
        return getattr(api, action)()
    else:
        raise ValueError(f"Unrecognized Manifest: {manifest_name}")
