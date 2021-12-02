#!/usr/bin/env python
# -*- coding: UTF-8 -*-


import os
import time

import pandas as pd
from pandas import DataFrame

from base import BaseObject


class InvertCollectionVectorSpace(BaseObject):
    """
    Purpose:
        Invert the Vector Space by Term
    Implementation:
        -   The output is keyed by Term
            and each term is a list of values
            where each value is a key field in the underlying collection
        -   For example, in a vector space across supply_tag_<date>
            each term has a list of serial numbers
            and those serial numbers represent individuals for whom that term
                is a discriminating skill
    Sample Output:
        +------+------------+-------------------------------------------+
        |      | KeyField   | Tag                                       |
        |------+------------+-------------------------------------------|
        |    0 | 0697A5744  | cloud service                             |
        |    1 | 0697A5744  | data science                              |
        |    2 | 0697A5744  | solution design                           |
        |    3 | 05817Q744  | kubernetes                                |
        |    4 | 05817Q744  | bachelor of engineering                   |
        |    5 | 05817Q744  | developer                                 |
        ...
        | 1328 | 249045760  | electrical engineering                    |
        +------+------------+-------------------------------------------+
    """

    __reader = None

    def __init__(self,
                 library_name: str,
                 is_debug: bool = False):
        """
        Created:
            4-Nov-2019
            craig.trim@ibm.com
            *   https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1261#issuecomment-15732844
        """
        BaseObject.__init__(self, __name__)

        self._is_debug = is_debug
        self._library_name = library_name

    def _file_path(self):
        fname = f"{self._library_name.split('.csv')[0].strip()}_INVERTED"
        return os.path.join(os.environ['GTS_BASE'],
                            f"resources/confidential_input/vectorspace/{fname}.csv")

    def _key_fields(self):
        from cendalytics.tfidf.core.dmo import VectorSpaceLibraryLoader

        df = VectorSpaceLibraryLoader(is_debug=self._is_debug,
                                      library_name=self._library_name).df()
        return df['Doc'].unique()

    def _to_dataframe(self,
                      top_n: int,
                      key_fields: list) -> DataFrame:
        results = []
        for key_field in key_fields:
            df_top = self.__reader.process(key_field=key_field,
                                           top_n=top_n)

            for tag in df_top['Term'].unique():
                results.append({
                    "KeyField": key_field,
                    "Term": tag.lower()})

        return pd.DataFrame(results)

    def __init_reader(self):
        if not self.__reader:
            from cendalytics.tfidf.core.svc import ReadCollectionVectorSpace
            self.__reader = ReadCollectionVectorSpace(is_debug=False,  # maintain as False
                                                      library_name=self._library_name)

    def process(self,
                top_n: int = 3) -> str:
        """
        :param top_n:
        :return:
            the file path
        """

        start = time.time()

        self.__init_reader()
        fpath = self._file_path()
        key_fields = self._key_fields()

        df = self._to_dataframe(top_n=top_n,
                                key_fields=key_fields)

        df.to_csv(fpath,
                  encoding='utf-8',
                  sep='\t')

        if self._is_debug:
            end_time = round(time.time() - start, 2)
            self.logger.debug('\n'.join([
                "VectorSpace Inversion Complete",
                f"\tFile Path: {fpath}",
                f"\tTotal Time: {end_time}s",
                f"\tTotal Records: {len(df)}"]))

        return fpath
