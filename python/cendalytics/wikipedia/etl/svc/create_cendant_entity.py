#!/usr/bin/env python
# -*- coding: UTF-8 -*-


import codecs
from typing import Optional

import jsonpickle

from base import BaseObject
from base import FileIO
from base import LabelFormatter
from base import RedisClient
from cendalytics.wikipedia.ingest.dmo import DBpediaEntityResolution
from cendalytics.wikipedia.ingest.dmo import DBpediaTaxonomyExtractor
from cendalytics.wikipedia.ingest.svc import FindDbPediaEntryRedis
from cendalytics.wikipedia.ingest.svc import PostProcessDBPediaPageRedis
from taskmda import AugmentationAPI

RECURSION_THRESHOLD_MAX = 2


class CreateCendantEntity(BaseObject):
    """ Transform a DBPedia entry to one-or-more Cendant entities """

    __d_owl_entries = {}
    __entity_blacklist = None

    __prefix = "wiki_page_"
    __version_info = "Cendant/dbPedia Automation"

    def __init__(self,
                 ignore_cache: bool = False,
                 invalidate_cache: bool = False,
                 is_debug: bool = False):
        """
        Created:
            17-Jan-2020
            craig.trim@ibm.com
            *   refactored from script 'create-ontology-entry'
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1761
        Updated:
            6-Feb-2020
            craig.trim@ibm.com
            *   refactored
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1827
        """
        BaseObject.__init__(self, __name__)
        if invalidate_cache:
            self.__d_owl_entries = {}

        if self.__entity_blacklist is None:
            self.__entity_blacklist = self._load_blacklist()

        self._is_debug = is_debug
        self._ignore_cache = ignore_cache
        self._augapi = AugmentationAPI(is_debug=self._is_debug)
        self._redis = RedisClient(RedisClient.WIKI_AUGMENTED_DB)
        self._finder = FindDbPediaEntryRedis(is_debug=self._is_debug)
        self._processor = PostProcessDBPediaPageRedis(is_debug=self._is_debug)

    @staticmethod
    def _load_blacklist() -> list:
        path = "resources/config/dbpedia/entity_blacklist.csv"
        blacklist = FileIO.file_to_lines_by_relative_path(path)
        blacklist = [x.lower().strip() for x in blacklist]

        return blacklist

    def _generate(self,
                  some_term: str) -> Optional[dict]:

        entry = self._finder.process(entity_name=some_term,
                                     ignore_cache=self._ignore_cache)

        if entry:
            return self._processor.process(entry=entry,
                                           ignore_cache=self._ignore_cache)

    @staticmethod
    def _clean_see_also(see_also: set,
                        d_page_title: str) -> list:
        see_also = [LabelFormatter.camel_case(x, split_tokens=True).strip()
                    for x in see_also if x]
        see_also = [x for x in see_also
                    if x.lower() != d_page_title.lower()]
        return sorted([x for x in see_also if x])

    def _result(self,
                some_term: str) -> dict:
        _key = f"{self.__prefix}{some_term}"
        if not self._redis.has(_key) or self._ignore_cache:
            return self._generate(some_term)
        return jsonpickle.decode(self._redis.get(_key))

    @staticmethod
    def _summary_input(d_page: dict) -> Optional[str]:
        if type(d_page['summary']) == list:
            if len(d_page['summary']):
                return d_page['summary'][0]
            return None

        return d_page['summary']['first']

    def _subclass_finder(self,
                         d_page: dict) -> Optional[str]:
        """
        Purpose:
            Extract a subClass from unstructured text
        Sample Input:
            "alpha is a type of beta"
        Sample Output
            "beta"
        :param d_page:
            any dbPedia entry
        :return:
            Optional        an extracted subclass
        """
        return DBpediaTaxonomyExtractor(is_debug=self._is_debug,
                                        input_text=self._summary_input(d_page)).process()

    def _entity_finder(self,
                       some_name: str) -> Optional[str]:
        """
        Purpose:
            Find a dbPedia Entry for a given Term
        Notes:
            This is not an exact science as many terms are highly ambiguous
            in the case of multiple results we take the most likely match
        Sample Input:
            "beta"
        Sample Output:
            "Beta Tech"
        :param some_name:
            any input
        :return:
            a dbpedia entry
        """
        return DBpediaEntityResolution(is_debug=self._is_debug,
                                       some_title=some_name).most_likely_result()

    __d_term2page = {}

    def _generate_entry(self,
                        some_term: str,
                        see_also: set,
                        recursion_ctr: int) -> None:
        from cendalytics.wikipedia.etl.dmo import ImplicationFinder
        from cendalytics.wikipedia.etl.dmo import PartonomyFinder

        if recursion_ctr > RECURSION_THRESHOLD_MAX:
            return
        elif some_term in self.__d_term2page:
            return

        d_page = self._result(some_term)
        self.__d_term2page[some_term] = d_page

        # Find SubClass in Text
        sub_class_1 = self._subclass_finder(d_page)
        if sub_class_1 and sub_class_1.lower() in self.__entity_blacklist:
            return

        self.logger.debug('\n'.join([
            f"Processing:",
            f"\tSub Class: {sub_class_1}",
            f"\tIncoming Term: {some_term}"]))

        # Resolve SubClass to a dbPedia Entity
        sub_class_2 = None
        if sub_class_1 is not None:

            sub_class_2 = self._entity_finder(sub_class_1)
            if sub_class_2 and sub_class_2.lower() in self.__entity_blacklist:
                return

            if sub_class_2 and d_page['title'].lower() == sub_class_2.lower():
                if self._is_debug:
                    self.logger.debug('\n'.join([
                        "Warning: Matching Entry and SubClass",
                        f"\tEntry: {d_page['title']}",
                        f"\tSubClass (Input): {sub_class_1}",
                        f"\tSubClass (Resolved): {sub_class_2}",
                        f"\tTerm: {some_term}"]))

                # SubClass is equivalent to Current Term
                # thus SubClass becomes a synonym of Current Term
                see_also.add(sub_class_1)
                see_also.add(sub_class_2)
                sub_class_2 = None

        see_also.add(some_term)
        see_also = self._clean_see_also(see_also, d_page['title'])

        part_of_results = PartonomyFinder(d_page=d_page,
                                          is_debug=self._is_debug).process()
        part_of_results = [x for x in part_of_results
                           if x != some_term]
        part_of_results = [x for x in part_of_results
                           if x.lower() not in self.__entity_blacklist]

        for part_of_result in part_of_results:
            self._generate_entry(some_term=part_of_result,
                                 see_also=set(),
                                 recursion_ctr=recursion_ctr + 1)

        d_implications = ImplicationFinder(input_text=self._summary_input(d_page),
                                           candidate_matches=d_page['links'],
                                           is_debug=self._is_debug).process()
        for implication in d_implications:
            self._generate_entry(some_term=implication,
                                 see_also=d_implications[implication],
                                 recursion_ctr=recursion_ctr + 1)

        d_owl = self._augapi.entities(terms=[d_page['title']],
                                      sub_class=sub_class_2,
                                      is_defined_by=d_page['url'],
                                      version_info=self.__version_info,
                                      see_also=sorted(see_also),
                                      part_of=sorted(part_of_results),
                                      implications=sorted(d_implications.keys()),
                                      comment=self._summary_input(d_page))[0]

        self.__d_owl_entries[d_page['title']] = d_owl
        target = codecs.open("/Users/craig.trimibm.com/Desktop/cendant-results.txt",
                             encoding="utf-8", mode="a")
        target.write("\n\n")
        target.write(d_owl)
        target.write("\n\n")
        target.close()

        if sub_class_2:
            def _see_also() -> set:
                return set([x for x in {sub_class_1, sub_class_2} if x])

            self._generate_entry(some_term=sub_class_2,
                                 see_also=_see_also(),
                                 recursion_ctr=recursion_ctr + 1)

    def process(self,
                a_term: str) -> dict:

        self._generate_entry(a_term, set(), 0)

        self.logger.debug('\n'.join([
            "Cendant Entity Generation Completed",
            f"\tTotal Results: {len(self.__d_owl_entries)}"]))
        print('------------------------------------------------')
        for entry in self.__d_owl_entries.values():
            print(entry)
            print('\n')
        print('------------------------------------------------')

        return self.__d_owl_entries


def main(param_term):
    CreateCendantEntity(is_debug=True).process(param_term)


if __name__ == "__main__":
    import plac

    plac.call(main)
