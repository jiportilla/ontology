# !/usr/bin/env python
# -*- coding: UTF-8 -*-


from pandas import DataFrame

from base import BaseObject
from datagit.graph.dmo import FileCommitNodeBuilder
from dataingest import PythonParseAPI


class GitHubRecordFinder(BaseObject):
    """ Find the appropriate GitHub Record """

    __df_src = None

    def __init__(self,
                 pattern: dict,
                 d_index: dict,
                 is_debug: bool = True):
        """
        Created:
            31-Dec-2019
            craig.trim@ibm.com
            *   refactored out of 'graph-github-issue' and 'graph-social-network'
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1681#issuecomment-16877247
        :param is_debug:
            if True     increase log output at DEBUG level
        """
        BaseObject.__init__(self, __name__)
        self._index = d_index
        self._pattern = pattern
        self._is_debug = is_debug
        api = PythonParseAPI(is_debug=self._is_debug)
        if self.__df_src is None:
            self.__df_src = api.load(date="20191224").source()

    def find(self):

        class Facade(object):

            def __init__(self,
                         df_src: DataFrame):
                self._df_src = df_src

            @staticmethod
            def issue() -> dict:
                return self._index[self._pattern['issue']]

            @staticmethod
            def assignments() -> list:
                if 'assignments' in self._pattern:
                    return [self._index[assignment]
                            for assignment in self._pattern['assignments']]

            def has_assignment(self) -> bool:
                return len(self.assignments()) > 0

            @staticmethod
            def comments() -> list:
                if 'comments' in self._pattern:
                    return [self._index[assignment]
                            for assignment in self._pattern['comments']]

            def has_comments(self) -> bool:
                return len(self.comments()) > 0

            @staticmethod
            def pull_requests() -> list:
                return [pull_request for pull_request in self._pattern['pull-requests']
                        if len(pull_request['files']) > 0]

            @staticmethod
            def commit(pull_request: dict) -> dict:
                return self._index[pull_request['commit']]

            @staticmethod
            def files(pull_request: dict) -> list:
                files = [self._index[file_id]
                         for file_id in pull_request['files']]

                # restrict to '.py' files only; GIT-1648-16817935
                def _field_value(a_record: dict):
                    return [field['value'] for field in a_record["fields"]
                            if field["name"].lower() == 'file'][0]

                files = [file for file in files
                         if _field_value(file).endswith('.py')]

                return files

            def file_records(self,
                             filecommit_nb: FileCommitNodeBuilder) -> DataFrame:
                # Must have matching filename
                df2 = self._df_src[self._df_src['FileName'] == filecommit_nb.file_name]

                # Must be valid Python file
                return df2[df2['FileName'].str.endswith(".py", na=False)]

        return Facade(df_src=self.__df_src)
