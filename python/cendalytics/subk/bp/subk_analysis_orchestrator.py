# !/usr/bin/env python
# -*- coding: UTF-8 -*-


from pandas import DataFrame

from base import BaseObject
from dataingest import ExcelReader


class SubkAnalysisOrchestrator(BaseObject):
    """ Orchestrate the Subk (Sub-Contractor) Analysis Business Process """

    def __init__(self,
                 input_spreadsheet:str,
                 is_debug: bool = True):
        """
        Created:
            16-Jan-2020
            craig.trim@ibm.com
            *   Reference
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1740#issuecomment-17198891
        :param is_debug:
            if True     increase log output at DEBUG level
        """
        BaseObject.__init__(self, __name__)
        self._is_debug = is_debug
        self._input_spreadsheet = input_spreadsheet

    def process(self,
                overwrite_files:bool=True):
        from cendalytics.subk.svc import AnalyzeSkillsForSubk
        from cendalytics.subk.svc import ClusterInputSkills
        from cendalytics.subk.svc import ClusterSkillsForGTS

        AnalyzeSkillsForSubk(is_debug=self._is_debug,
                             input_file=self._input_spreadsheet).process()




def main():
    IS_DEBUG = True
    input_spreadsheet = "/Users/craig.trimibm.com/Box/GTS CDO Workforce Transformation/04. Documentation/Cendant Tasks/GIT-1740/GTS Labs Subk's - skill details.xlsx"
    orchestrator = SubkAnalysisOrchestrator(is_debug=IS_DEBUG,
                                            input_spreadsheet=input_spreadsheet)
    orchestrator.process(overwrite_files=False)



if __name__ == "__main__":
    main()
