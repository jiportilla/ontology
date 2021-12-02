#!/usr/bin/env python
# -*- coding: UTF-8 -*-


from cendalytics.wikipedia.ingest.svc import CompareDBPediaEntries


def main(term_1, term_2):
    IS_DEBUG = True

    CompareDBPediaEntries(entity_name_1=term_1,
                          entity_name_2=term_2,
                          ontology_name='biotech',
                          is_debug=IS_DEBUG).process()


if __name__ == "__main__":
    import plac

    plac.call(main)
