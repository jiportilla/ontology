# !/usr/bin/env python
# -*- coding: UTF-8 -*-


from typing import Optional

from base import BaseObject


class NavigateGitHubRecords(BaseObject):
    """
    Purpose:
    Navigate the records in GitHub

    Traceability:
    https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1542

    Prereq:
    A GitHub Dependency Parse Analysis
    """

    __build_issue_index = None
    __build_issue_taxonomy = None

    def __init__(self,
                 collection_name: str,
                 is_debug: bool = True):
        """
        Created:
            6-Dec-2019
            craig.trim@ibm.com
            *   https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1542
        Updated:
            25-Dec-2019
            craig.trim@ibm.com
            *   process flow optimizations in pursuit of
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1651
        :param is_debug:
            if True     increase log output at DEBUG level
        """
        BaseObject.__init__(self, __name__)
        from datagit.navigate.dmo import IssueIndexBuilder
        from datagit.navigate.dmo import IssueTaxonomyBuilder

        self._is_debug = is_debug
        if self.__build_issue_index is None:
            self.__build_issue_index = IssueIndexBuilder(
                is_debug=self._is_debug,
                collection_name=collection_name)
        if self.__build_issue_taxonomy is None:
            self.__build_issue_taxonomy = IssueTaxonomyBuilder(
                is_debug=self._is_debug)

        if self._is_debug:
            self.logger.debug('\n'.join([
                "Instantiated NavigateGitHubRecords",
                f"\tCollection Name: {collection_name}"]))

    def process(self,
                issue_id: str) -> Optional[dict]:
        from datagit.navigate.dmo import IssuePatternBuilder

        d_index = self.__build_issue_index.process(key_field=issue_id)
        if not d_index:
            return None

        df_taxonomy = self.__build_issue_taxonomy.process(d_index=d_index)
        if df_taxonomy.empty:
            return None

        pattern = IssuePatternBuilder(d_index=d_index,
                                      df_taxonomy=df_taxonomy,
                                      key_field=issue_id,
                                      is_debug=self._is_debug).process()

        svcresult = {
            "index": d_index,
            "pattern": pattern}

        return svcresult
