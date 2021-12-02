# !/usr/bin/env python
# -*- coding: UTF-8 -*-


from base import BaseObject


class GitHubAnalysisOrchestrator(BaseObject):
    """ Orchestration of Services for GitHub Analysis """

    def __init__(self,
                 is_debug: bool = True):
        """
        Created:
            2-Jan-2020
            craig.trim@ibm.com
            *   https://github.ibm.com/-cdo/unstructured-analytics/issues/1680#issuecomment-16901240
        :param is_debug:
            if True     increase log output at DEBUG level
        """
        BaseObject.__init__(self, __name__)

        self._is_debug = is_debug

    def distributions(self,
                      collection_name: str):
        from datagit.analyze.svc import GenerateCommitDistribution
        from datagit.analyze.svc import GenerateFileCommitDistribution
        from datagit.analyze.svc import GenerateSocialCollocation
        from datagit.analyze.svc import GenerateSocialDistribution

        class Facade(object):

            @staticmethod
            def commit() -> None:
                return GenerateCommitDistribution(is_debug=self._is_debug,
                                                  collection_name=collection_name).process()

            @staticmethod
            def filecommit() -> None:
                return GenerateFileCommitDistribution(is_debug=self._is_debug,
                                                      collection_name=collection_name).process()

            @staticmethod
            def social(write_to_file: bool = True) -> dict:
                svcresult_1 = GenerateSocialCollocation(is_debug=self._is_debug,
                                                        collection_name=collection_name).process(write_to_file)
                svcresult_2 = GenerateSocialDistribution(is_debug=self._is_debug,
                                                         df_rel_input=svcresult_1["rel"],
                                                         df_ent_input=svcresult_1["ent"]).process(write_to_file)

                return svcresult_2

        return Facade()
