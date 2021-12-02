#!/usr/bin/env python
# -*- coding: UTF-8 -*-


from typing import Optional

from base import BaseObject
from nlusvc import TextAPI


class PostProcessDBPediaPage(BaseObject):
    """ Post Process a DBPedia structure """

    __ignore = {  # circular references within dbPedia
        'area code': 'Area codes in Mexico by code'
    }

    def __init__(self,
                 entry: dict,
                 is_debug: bool = False):
        """
        Created:
            8-Jan-2020
            craig.trim@ibm.com
            *   https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1710#issuecomment-17016514
        """
        BaseObject.__init__(self, __name__)

        self._entry = entry
        self._is_debug = is_debug
        self._text_api = TextAPI(is_debug=False)

    def _filter_categories(self,
                           categories: list):
        from cendalytics.wikipedia.ingest.dmo import DBpediaCategoryFilter

        return DBpediaCategoryFilter(categories,
                                     maximum_token_threshold=3,
                                     is_debug=self._is_debug).process()

    def _filter_references(self,
                           references: list):
        from cendalytics.wikipedia.ingest.dmo import DBpediaReferencesFilter

        return DBpediaReferencesFilter(references,
                                       is_debug=self._is_debug).process()

    def _subclassof(self,
                    input_text: str) -> str:
        from cendalytics.wikipedia.ingest.dmo import DBpediaTaxonomyExtractor

        return DBpediaTaxonomyExtractor(input_text=input_text,
                                        is_debug=self._is_debug).process()

    def _seealso_extractor(self,
                           content: list) -> list:
        from cendalytics.wikipedia.ingest.dmo import DBpediaSeeAlsoExtractor
        result = DBpediaSeeAlsoExtractor(content=content,
                                         is_debug=self._is_debug).process()
        if result and type(result) == str:
            return [result]

        if not result:
            return []

        raise NotImplementedError

    def _total_matching_categories(self,
                                   entity_name: str,
                                   categories: set) -> int:
        from cendalytics.wikipedia.ingest.svc import FindDbPediaEntryRedis
        from cendalytics.wikipedia.ingest.svc import PostProcessDBPediaPageRedis

        _finder = FindDbPediaEntryRedis(is_debug=self._is_debug)
        _postprocess = PostProcessDBPediaPageRedis(is_debug=self._is_debug)

        entity_1 = _finder.process(entity_name)
        if not entity_1:
            return 0

        entity_2 = _postprocess.process(entry=entity_1,
                                        ignore_cache=False)
        if not entity_2:
            return 0

        s_categories = set(entity_2['categories'])
        if not s_categories or not len(s_categories):
            return 0

        return len(sorted(categories.intersection(s_categories)))

    def _resolve_entity_name(self,
                             an_entity: str) -> str:
        from cendalytics.wikipedia.ingest.dmo import DBpediaEntityResolution
        return DBpediaEntityResolution(is_debug=self._is_debug,
                                       some_title=an_entity).most_likely_result()

    def _augment_content(self,
                         content: list) -> dict:

        if not content or not len(content):
            return {
                "content": [],
                "cased_terms": [],
                "first": None,
                "sentences": 0}

        cased_terms = self._text_api.cased_terms(content)

        first = content[0]

        ctr = 0
        while '(' in first and ')' in first:
            x = first.index('(')
            y = first.index(')')
            first = f"{first[:x]}{first[y + 1:]}"
            ctr += 1
            if ctr > 5:
                break

        while '  ' in first:
            first = first.replace('  ', ' ')

        return {
            "content": content,
            "cased_terms": cased_terms,
            "first": first,
            "sentences": len(content)}

    def process(self):

        self.logger.debug('\n'.join([
            'Instantiated PostProcessor',
            f"\tEntry: {self._entry['title']}"]))

        master = self._entry

        _content = self._entry['content']
        _summary = self._entry['summary']

        _categories = self._entry['categories']
        _references = self._entry['references']

        master['content'] = self._augment_content(content=_content)
        master['summary'] = self._augment_content(content=_summary)

        master['see_also'] = self._seealso_extractor(_content)
        master['categories'] = self._filter_categories(_categories)
        master['references'] = self._filter_references(_references)
        master['sub_class_of'] = self._subclassof(master['summary']['first'])

        return master
