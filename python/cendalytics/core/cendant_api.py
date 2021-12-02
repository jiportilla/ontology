#!/usr/bin/env python
# -*- coding: UTF-8 -*-


from base import BaseObject
from cendalytics.education.bp import EducationAnalysisAPI
from cendalytics.inference.bp import InferenceAPI
from cendalytics.skills.core.bp import SkillsReportAPI
from cendalytics.tfidf.core.bp import VectorSpaceAPI


class CendantAPI(BaseObject):
    """ Cendant API """

    def __init__(self,
                 is_debug: bool = False):
        """
        Created:
            6-Nov-2019
            craig.trim@ibm.com
        """
        BaseObject.__init__(self, __name__)
        self._is_debug = is_debug

    def inference(self) -> InferenceAPI:
        return InferenceAPI(is_debug=self._is_debug)

    def skills(self) -> SkillsReportAPI:
        return SkillsReportAPI(is_debug=self._is_debug)

    def vectorspace(self) -> VectorSpaceAPI:
        return VectorSpaceAPI(is_debug=self._is_debug)

    def education(self) -> EducationAnalysisAPI:
        return EducationAnalysisAPI(is_debug=self._is_debug)


def main():
    pass


if __name__ == "__main__":
    main()
