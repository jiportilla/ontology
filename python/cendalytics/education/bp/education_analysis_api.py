#!/usr/bin/env python
# -*- coding: UTF-8 -*-


from base import BaseObject
from cendalytics.education.svc import PerformEducationAnalysis
from datamongo.core.dmo import BaseMongoClient


class EducationAnalysisAPI(BaseObject):
    """ Collection Wrapper over MongoDB Collection for 'cv_ingest_employee_education'  """

    _records = []
    _cache = {}

    def __init__(self,
                 limit: int = None,
                 some_base_client: BaseMongoClient = None,
                 is_debug: bool = False):
        """
        Created:
            23-Apr-2019
            craig.trim@ibm.com
        Updated:
            17-Oct-2019
            craig.trim@ibm.com
            *   renamed from '-employees-education' and moved out of datamongo
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1142
        :param some_base_client:
        :param is_debug:
        """
        BaseObject.__init__(self, __name__)

        if not some_base_client:
            some_base_client = BaseMongoClient()

        self._limit = limit
        self._is_debug = is_debug
        self._mongo_client = some_base_client

        if self._is_debug:
            self.logger.debug('\n'.join([
                "Instantiate EducationAnalysisAPI",
                f"\tLimit: {self._limit}"]))

    def count(self,
              collection_name: str) -> PerformEducationAnalysis:
        return PerformEducationAnalysis(limit=self._limit,
                                        collection_name=collection_name,
                                        some_base_client=self._mongo_client,
                                        is_debug=self._is_debug)


def main(a_collection_name, outfile, limit):
    def get_limit() -> int or None:
        if not limit:
            return None
        try:
            return int(limit)
        except ValueError:
            return None

    api = EducationAnalysisAPI(limit=get_limit(), is_debug=True)
    df = api.count(a_collection_name).degrees()
    df.to_csv(outfile, sep='\t', encoding='utf-8')


if __name__ == "__main__":
    import plac

    plac.call(main)
