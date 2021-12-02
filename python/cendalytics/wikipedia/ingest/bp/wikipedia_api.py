#!/usr/bin/env python
# -*- coding: UTF-8 -*-


class WikipediaAPI:

    @classmethod
    def find_dbpedia_entry(cls,
                           some_title: str):
        from cendalytics.wikipedia.ingest.svc import FindDbPediaEntry
        return FindDbPediaEntry(some_input=some_title).entry()

    @classmethod
    def find_dbpedia_entries_for_cendant(cls):
        from cendalytics.wikipedia.ingest.svc import FindDbPediaEntriesForCendant
        FindDbPediaEntriesForCendant(is_debug=True).process()


def main(param1):
    param1 = param1.lower().strip()

    if param1 == "lookup":
        WikipediaAPI.find_dbpedia_entries_for_cendant()
    else:
        raise NotImplementedError("\n".join([
            "Wikipedia Action Not Implemented (name={})".format(
                param1)]))


if __name__ == "__main__":
    import plac

    plac.call(main)
