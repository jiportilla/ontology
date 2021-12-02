#!/usr/bin/env python
# -*- coding: UTF-8 -*-


from taskmda import AugmentationAPI

IS_DEBUG = True
IGNORE_CACHE = True
ONTOLOGY_NAME = "biotech"


def main(some_term):
    api = AugmentationAPI(is_debug=True)
    api.redirects([some_term],
                  ontology_name=ONTOLOGY_NAME)


if __name__ == "__main__":
    import plac

    plac.call(main)
