#!/usr/bin/env python
# -*- coding: UTF-8 -*-


from typing import Optional

import jsonpickle

from base import BaseObject
from base import RedisClient


class PostProcessDBPediaPageRedis(BaseObject):
    """ for a given input find the associated dbPedia entry """

    _prefix = "wiki_page_"

    _redis = RedisClient(RedisClient.WIKI_AUGMENTED_DB)

    def __init__(self,
                 is_debug: bool = False):
        """
        Created:
            8-Jan-2020
            craig.trim@ibm.com
            *   moved redis to a separate service
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1710#issuecomment-17015882
        """
        BaseObject.__init__(self, __name__)
        self._is_debug = is_debug

    def process(self,
                entry: dict,
                ignore_cache: bool = False) -> Optional[dict]:
        from cendalytics.wikipedia.ingest.svc import PostProcessDBPediaPage

        _key = f"{self._prefix}{entry['title']}"
        if not self._redis.has(_key) or ignore_cache:
            entry = PostProcessDBPediaPage(entry=entry,
                                           is_debug=self._is_debug).process()
            self._redis.set(_key,
                            jsonpickle.encode(entry))
        else:
            self.logger.debug(f"Retrieved Entity (name={entry['title']})")

        return jsonpickle.decode(self._redis.get(_key))
