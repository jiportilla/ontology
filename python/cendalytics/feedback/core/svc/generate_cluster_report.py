#!/usr/bin/env python
# -*- coding: UTF-8 -*-


from collections import Counter

from base import BaseObject
from datadict import FindDimensions
from datamongo import CendantCollection


class GenerateClusterReport(BaseObject):
    """  """

    __dim_cache = {}

    def __init__(self,
                 collection_name: str,
                 is_debug: bool = False):
        """
        Created:
            22-Nov-2019
            craig.trim@ibm.com
            *   https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1419#issuecomment-16186525
        :param collection_name:
        :param is_debug:
        """
        BaseObject.__init__(self, __name__)

        self._is_debug = is_debug
        self._dim_finder = FindDimensions.sentiment(is_debug=False)
        self._collection = CendantCollection(is_debug=self._is_debug,
                                             some_collection_name=collection_name)
        self._records = self._load_records()

    @staticmethod
    def _tags(field: dict,
              confidence_threshold: float = 55.0) -> set:
        return set([tag[0] for tag in field["tags"]["supervised"]
                    if tag[1] >= confidence_threshold])

    def _all_tags(self,
                  record: dict):
        tags = set()
        long_text_fields = [field for field in record["fields"] if field["type"] == "long-text"]
        for field in long_text_fields:
            tags = tags.union(self._tags(field))
        return tags

    def _dim_finder_cache(self,
                          tag: str) -> list:
        if tag in self.__dim_cache:
            return self.__dim_cache[tag]
        self.__dim_cache[tag] = self._dim_finder.find(tag)
        return self.__dim_cache[tag]

    def _load_records(self) -> list:
        records = self._collection.all()
        if self._is_debug:
            self.logger.debug('\n'.join([
                f"Retrieved Records (total={len(records)})",
                f"\tCollection Name: {self._collection.collection_name}"]))

        return records

    def clusters(self):
        class Facade(object):

            @staticmethod
            def tags(level: int):
                return self._do_tag_clustering(level=level)

            @staticmethod
            def schema():
                pass

        return Facade()

    def _do_tag_clustering(self,
                           level: int):
        def algo():
            if level == 2:
                return self._bigram_tag_cluster_algo
            if level == 3:
                return self._trigram_tag_cluster_algo
            if level == 4:
                return self._quadgram_tag_cluster_algo
            raise NotImplementedError

        return self._tag_cluster_action(cluster_algo=algo())

    @staticmethod
    def _bigram_tag_cluster_algo(tags: list,
                                 c: Counter):
        if len(tags) < 2:
            return

        for t1 in tags:
            for t2 in tags:
                if len({t1, t2}) == 2:
                    term = ', '.join(sorted({t1, t2}))
                    c.update({term: 1})

    @staticmethod
    def _trigram_tag_cluster_algo(tags: list,
                                  c: Counter):
        if len(tags) < 3:
            return

        for t1 in tags:
            for t2 in tags:
                for t3 in tags:
                    if len({t1, t2, t3}) == 3:
                        term = ', '.join(sorted({t1, t2, t3}))
                        c.update({term: 1})

    @staticmethod
    def _quadgram_tag_cluster_algo(tags: list,
                                   c: Counter):
        if len(tags) < 4:
            return

        for t1 in tags:
            for t2 in tags:
                for t3 in tags:
                    for t4 in tags:
                        if len({t1, t2, t3, t4}) == 4:
                            term = ', '.join(sorted({t1, t2, t3, t4}))
                            c.update({term: 1})

    def _tag_cluster_action(self,
                            cluster_algo,
                            log_threshold: int = 5000):

        ctr = 0

        c = Counter()
        total_records = len(self._records)

        for record in self._records:
            ctr += 1
            if ctr % log_threshold == 0:
                self.logger.debug(f"Status: {ctr}-{total_records}")

            tags = self._all_tags(record)
            cluster_algo(tags=tags, c=c)

        return c


def main():
    report = GenerateClusterReport(is_debug=True, collection_name='feedback_tag_20191122')
    c = report.clusters().tags(level=2)
    print(c.most_common(100))


if __name__ == "__main__":
    main()
