#!/usr/bin/env python
# -*- coding: UTF-8 -*-


import logging
import pprint

from cendalytics.wikipedia.ingest.svc import FindDbPediaEntryRedis
from cendalytics.wikipedia.ingest.svc import PostProcessDBPediaPageRedis


def main(a_term):
    IS_DEBUG = False

    logger = logging.getLogger('connectionpool')
    logger.setLevel(logging.INFO)

    finder = FindDbPediaEntryRedis(is_debug=IS_DEBUG)
    processor = PostProcessDBPediaPageRedis(is_debug=IS_DEBUG)

    print (f"Term: {a_term}")

    def _entry(some_term: str) -> dict:
        entry = finder.process(entity_name=some_term,
                               ignore_cache=True)
        if entry:
            return processor.process(entry=entry,
                                     ignore_cache=True)

    # def _links(an_entry: dict):
    # for link in an_entry['links']:
    #     _entry(link)
    # for link in an_entry['links']:
    #     _links(_entry(link))

    # _links(_entry(a_term))
    pprint.pprint(_entry(a_term))


if __name__ == "__main__":
    import plac

    plac.call(main)
