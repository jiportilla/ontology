#!/usr/bin/env python
# -*- coding: UTF-8 -*-


from typing import Optional

import jsonpickle

from base import BaseObject
from base import RedisClient


class FindDbPediaEntryRedis(BaseObject):
    """ for a given input find the associated dbPedia entry """

    _prefix = "wiki_page_"

    _redis = RedisClient(RedisClient.WIKI_PAGE_DB)

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
                entity_name: str,
                ignore_cache: bool = False) -> Optional[dict]:
        from cendalytics.wikipedia.ingest.svc import FindDbPediaEntry

        _key = self._prefix + entity_name

        if not self._redis.has(_key) or ignore_cache:
            try:
                entry = FindDbPediaEntry(is_debug=self._is_debug).process(entity_name)
                if not entry:
                    return None

                self._redis.set(_key, jsonpickle.encode(entry))
            except Exception as e:
                self.logger.exception(e)
                return None

        else:
            self.logger.debug(f"Retrieved Entity (name={entity_name}, ignore-cache={ignore_cache})")

        return jsonpickle.decode(self._redis.get(_key))
