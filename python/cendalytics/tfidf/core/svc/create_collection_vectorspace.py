#!/usr/bin/env python
# -*- coding: UTF-8 -*-


import os
import pprint
import time

from pandas import DataFrame

from base import BaseObject
from base import FileIO
from datamongo import BaseMongoClient


class CreateCollectionVectorSpace(BaseObject):
    """
    Purpose:
        Generate a Vector Space using TFIDF across a given collection

    Sample Output:
        +-------+-----------+----------------+----------+---------+-----------+---------------------+-------------------------+----------------------+--------------+-------------+
        |       | Doc       |   DocsWithTerm |      IDF |      TF |     TFIDF | Term                |   TermFrequencyInCorpus |   TermFrequencyInDoc |   TermsInDoc |   TotalDocs |
        |-------+-----------+----------------+----------+---------+-----------+---------------------+-------------------------+----------------------+--------------+-------------|
        |     0 | 0697A5744 |            159 |  2.39904 | 0.06667 |   0.02779 | cloud service       |                     186 |                    1 |           15 |        1751 |
        |     1 | 0697A5744 |           1094 |  0.47035 | 0.06667 |   0.14174 | management          |                    2573 |                    1 |           15 |        1751 |
        |     2 | 0697A5744 |           2006 | -0.13596 | 0.06667 |  -0.49036 | agile               |                    2194 |                    1 |           15 |        1751 |
        |     3 | 0697A5744 |           2995 | -0.53676 | 0.06667 |  -0.1242  | ibm                 |                    5857 |                    1 |           15 |        1751 |
        |     4 | 0697A5744 |            513 |  1.22767 | 0.06667 |   0.0543  | data science        |                     745 |                    1 |           15 |        1751 |
        ...
        | 97480 | 04132K744 |            479 |  1.29624 | 0.01754 |   0.01353 | maintenance         |                     945 |                    1 |           57 |        1751 |
        +-------+-----------+----------------+----------+---------+-----------+---------------------+-------------------------+----------------------+--------------+-------------+
    """

    def __init__(self,
                 collection_name: str,
                 division: str or None,
                 mongo_client: BaseMongoClient,
                 limit: int = 0,
                 is_debug: bool = False):
        """
        Created:
            4-Nov-2019
            craig.trim@ibm.com
            *   https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1261#issuecomment-15732844
        Updated:
            6-Nov-2019
            craig.trim@ibm.com
            *   https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1261#issuecomment-15797417
        """
        BaseObject.__init__(self, __name__)
        self._limit = limit
        self._is_debug = is_debug
        self._division = division
        self._mongo_client = mongo_client
        self._collection_name = collection_name

    def _tfidf(self) -> DataFrame:
        from cendalytics.tfidf.core.dmo import CollectionRecordRetrieval
        from cendalytics.tfidf.core.dmo import TfIdfComputer
        from cendalytics.tfidf.core.dmo import TermCountInCorpus
        from cendalytics.tfidf.core.dmo import TermCountByDocument
        from cendalytics.tfidf.core.dmo import DocIndexByTerm
        from cendalytics.tfidf.core.dmo import TermIndexByDocument

        source_records = CollectionRecordRetrieval(limit=self._limit,
                                                   division=self._division,
                                                   is_debug=self._is_debug,
                                                   mongo_client=self._mongo_client,
                                                   collection_name=self._collection_name).process()
        if not source_records or not len(source_records):
            raise ValueError("No Records Found")

        d_terms_by_document = TermIndexByDocument(is_debug=self._is_debug,
                                                  source_records=source_records).process()
        FileIO.text_to_file(os.path.join(os.environ['GTS_BASE'],
                                         f"resources/confidential_input/vectorspace/d_terms_by_document.csv"),
                            pprint.pformat(d_terms_by_document))

        d_documents_by_term = DocIndexByTerm(d_terms_by_document).process()
        FileIO.text_to_file(os.path.join(os.environ['GTS_BASE'],
                                         f"resources/confidential_input/vectorspace/d_documents_by_term.csv"),
                            pprint.pformat(d_documents_by_term))

        term_count_in_corpus = TermCountInCorpus(d_terms_by_document).process()
        FileIO.text_to_file(os.path.join(os.environ['GTS_BASE'],
                                         f"resources/confidential_input/vectorspace/term_count_in_corpus.csv"),
                            pprint.pformat(term_count_in_corpus))

        term_count_by_document = TermCountByDocument(d_terms_by_document).process()
        FileIO.text_to_file(os.path.join(os.environ['GTS_BASE'],
                                         f"resources/confidential_input/vectorspace/term_count_by_document.csv"),
                            pprint.pformat(term_count_by_document))

        return TfIdfComputer(d_terms_by_document=d_terms_by_document,
                             d_documents_by_term=d_documents_by_term,
                             term_count_by_document=term_count_by_document,
                             term_count_in_corpus=term_count_in_corpus).process()

    def _file_path(self):
        def _fname():
            if self._division:
                return f"{self._collection_name}_{self._division}_TFIDF".upper()
            return f"{self._collection_name}_TFIDF".upper()

        return os.path.join(os.environ['GTS_BASE'],
                            f"resources/confidential_input/vectorspace/{_fname()}.csv")

    def process(self) -> str:
        """
        :return:
            the file path
        """
        start = time.time()
        fpath = self._file_path()

        def inner_process() -> int:
            df = self._tfidf().sort_values(by=['Doc'], ascending=False)
            df.to_csv(fpath, encoding='utf-8', sep='\t')
            return len(df)

        try:

            total_records = inner_process()
            if self._is_debug:
                end_time = round(time.time() - start, 2)
                self.logger.debug('\n'.join([
                    "VectorSpace Creation Complete",
                    f"\tFile Path: {fpath}",
                    f"\tTotal Time: {end_time}s",
                    f"\tTotal Records: {total_records}"]))

        except ValueError as err:
            self.logger.error(err)

        return fpath
