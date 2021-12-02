#!/usr/bin/env python
# -*- coding: UTF-8 -*-


import pprint

import jsonpickle

from base import RedisClient
from taskmda import AugmentationAPI


def main(a_term):
    IS_DEBUG = True

    _augapi = AugmentationAPI(is_debug=IS_DEBUG)

    _redis = RedisClient(RedisClient.WIKI_AUGMENTED_DB)
    _prefix = "wiki_page_"

    _key = f"{_prefix}{a_term}"

    if not _redis.has(_key):
        print(f"Entity Not Found: {a_term}")
        return

    result = jsonpickle.decode(_redis.get(_key))
    pprint.pprint(result)


if __name__ == "__main__":
    import plac

    plac.call(main)
