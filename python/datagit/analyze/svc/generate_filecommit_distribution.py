# !/usr/bin/env python
# -*- coding: UTF-8 -*-


from statistics import mean
from statistics import stdev

from base import BaseObject
from datamongo import CendantCollection


class GenerateFileCommitDistribution(BaseObject):
    """ Analyze Distribution of Change Size for File Commits """

    def __init__(self,
                 collection_name: str,
                 is_debug: bool = True):
        """
        Created:
            26-Dec-2019
            craig.trim@ibm.com
            *   https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1661
        :param is_debug:
            if True     increase log output at DEBUG level
        """
        BaseObject.__init__(self, __name__)
        self._is_debug = is_debug
        self._collection = self._load_collection(collection_name)

    def _load_collection(self,
                         collection_name) -> CendantCollection:
        return CendantCollection(is_debug=self._is_debug,
                                 some_collection_name=collection_name)

    @staticmethod
    def _field_value_by_name(d_record: dict,
                             a_field_name: str) -> float:
        a_field_name = a_field_name.lower().strip()
        return [float(field['value'])
                for field in d_record['fields']
                if field['name'].lower().strip() == a_field_name][0]

    def _analyze_field_commits(self):
        file_commits = self._collection.by_field(field_name="div_field",
                                                 field_value="file-commit")
        changes = [self._field_value_by_name(file_commit, 'changes')
                   for file_commit in file_commits]

        m = mean(changes)
        s = stdev(changes)

        self.logger.debug('\n'.join([
            "Field Commit Analysis Completed",
            f"\tMean: {m}",
            f"\tStdev: {s}"]))

    def process(self):
        self._analyze_field_commits()
