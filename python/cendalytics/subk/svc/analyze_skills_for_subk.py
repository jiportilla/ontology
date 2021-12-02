# !/usr/bin/env python
# -*- coding: UTF-8 -*-


from pandas import DataFrame

from base import BaseObject
from dataingest import ExcelReader


class AnalyzeSkillsForSubk(BaseObject):
    """ Analyze Subk (Subcontractor) Skills listed in the Input Spreadsheet

        Analysis means:
        1.  Count Listed Skills
        2.  Parse Listed Skills
    """

    def __init__(self,
                 input_file: str,
                 is_debug: bool = True):
        """
        Created:
            15-Jan-2020
            craig.trim@ibm.com
            *   https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1740
        :param is_debug:
            if True     increase log output at DEBUG level
        """
        BaseObject.__init__(self, __name__)
        self._is_debug = is_debug
        self._input_file = input_file

    def _load_file(self) -> DataFrame:
        d_cols = {'CNUM': str,
                  'NotesID': str,
                  'Country': str,
                  'Skills': str}
        df = ExcelReader.read_excel(some_input_path=self._input_file,
                                    skiprows=1,
                                    column_names=d_cols,
                                    some_sheet_name="Sheet1")

        if self._is_debug:
            self.logger.debug('\n'.join([
                "Loaded File",
                f"\tInput Path: {self._input_file}",
                f"\tTotal Records: {len(df)}"]))

        return df

    def _skill_count_report(self,
                            df_input: DataFrame) -> DataFrame:
        from cendalytics.subk.dmo import SkillCountReport

        return SkillCountReport(is_debug=self._is_debug,
                                df_input=df_input).process()

    def _tag_analysis_report(self,
                             df_input: DataFrame) -> DataFrame:
        from cendalytics.subk.dmo import SkillParseReport

        return SkillParseReport(is_debug=self._is_debug,
                                df_input=df_input).process()

    def process(self) -> DataFrame:
        df_skill_input = self._load_file()

        df_skill_ctr = self._skill_count_report(
            df_input=df_skill_input)

        df_skill_tag = self._tag_analysis_report(
            df_input=df_skill_ctr)

        return df_skill_tag


def main():
    IS_DEBUG = True
    input_file = "/Users/craig.trimibm.com/Box/GTS CDO Workforce Transformation/04. Documentation/Cendant Tasks/GIT-1740 (Subcontractor Analysis)/GTS Labs Subk's - skill details.xlsx"
    AnalyzeSkillsForSubk(is_debug=IS_DEBUG,
                         input_file=input_file).process()


if __name__ == "__main__":
    main()
