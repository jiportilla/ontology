#!/usr/bin/env python
# -*- coding: UTF-8 -*-


from math import log

import pandas as pd
from pandas import DataFrame
from tabulate import tabulate

from base import BaseObject
from base import RecordUnavailableRecord
from cendalytics.tfidf.core.bp import VectorSpaceAPI
from cendalytics.tfidf.core.dmo import VectorSpaceTopNSelector
from datamongo import BaseMongoClient
from datamongo import CendantCollection


class CompareCnumAcrossDivisions(BaseObject):
    """
    Purpose:
        Given a Collection,
        1.  Create a Vector Space for the entire collection
            and invert this Vector Space
        2.  For each division represented in the Vector Space, create a Vector Space
            and invert this Vector Space
    """

    def __init__(self,
                 collection_name: str,
                 is_debug: bool = False):
        BaseObject.__init__(self, __name__)

        self._is_debug = is_debug
        self._collection_name = collection_name
        self._api = VectorSpaceAPI(is_debug=is_debug)

    def term_frequency(self,
                       serial_number: str,
                       df_cnum: DataFrame,
                       df_library: DataFrame) -> (DataFrame, set):

        results = []

        # are these terms highly discriminating?  or not valued?
        # either way, should capture
        terms_not_found = set()

        for term in sorted(df_cnum['Term'].unique()):

            df_cnum_term = df_cnum[df_cnum['Term'] == term]
            if df_cnum_term.empty:
                raise ValueError(f"Empty df_cnum_term: {term}")

            df_library_term = df_library[df_library['Term'] == term]
            if df_library_term.empty:  # not an error
                self.logger.info(f"Term Not Found: {term}")
                terms_not_found.add(term)
                continue

            # Number of times term t appears in a document
            term_frequency_in_doc = float(df_cnum_term['TermFrequencyInDoc'].unique()[0])

            # Total number of terms in the document
            terms_in_doc = float(df_cnum_term['TermsInDoc'].unique()[0])

            # TF: Term Frequency
            tf = term_frequency_in_doc / terms_in_doc

            # Total Documents in the comparison Libary
            total_documents = float(df_library_term['TotalDocs'].unique()[0])
            total_documents_2 = float(df_cnum_term['TotalDocs'].unique()[0])

            # Count the number of documents with term in the comparison Library
            documents_with_term = float(df_library_term['DocsWithTerm'].unique()[0])

            # Count the term frequency in the corpus
            term_frequency_in_corpus = float(df_library_term['TermFrequencyInCorpus'].unique()[0])

            # IDF: Inverse Document Frequency
            idf = log(total_documents / documents_with_term)

            # TFIDF
            tfidf = round(tf / idf, 5)
            tfidf_current = df_cnum_term['TFIDF'].unique()[0]

            results.append({
                "Doc": serial_number,
                "DocsWithTerm": documents_with_term,
                "IDF": idf,
                "TF": tf,
                "TFIDF": tfidf,
                "TFIDFOld": tfidf_current,
                "Term": term,
                "TermFrequencyInCorpus": term_frequency_in_corpus,
                "TermFrequencyInDoc": term_frequency_in_doc,
                "TermsInDoc": terms_in_doc,
                "TotalDocs": total_documents})

        return pd.DataFrame(results), terms_not_found

    def _divisions(self) -> list:
        mongo_client = BaseMongoClient()
        collection = CendantCollection(some_base_client=mongo_client,
                                       some_collection_name=self._collection_name)

        return collection.distinct("div_field")

    def process(self,
                serial_number: str):
        """

        :param serial_number:
        :return:
        """
        library_name = self._api.list().process(division=None,
                                                latest_only=True,
                                                library_type="vectorspace")
        self.logger.info(f"Library Retrieved ("
                         f"name={library_name})")

        df_library = self._api.tfidf().read(library_name).df()

        if self._is_debug:
            self.logger.debug('\n'.join([
                "Random Sample from Library",
                f"\tLibrary Name: {library_name}",
                tabulate(df_library.sample(3), headers='keys', tablefmt='psql')]))

        df_cnum = df_library[df_library['Doc'] == serial_number.upper()]
        if df_cnum.empty:
            raise RecordUnavailableRecord(f"No Records Found ("
                                          f"serial-number={serial_number})")

        self.logger.info('\n'.join([
            f"Located DataFrame by CNUM ({serial_number})",
            tabulate(df_cnum, headers='keys', tablefmt='psql')]))

        results = []
        unknown_terms = []

        for division in self._divisions():
            library_name_by_division = self._api.list().process(division=division,
                                                                latest_only=True,
                                                                library_type="vectorspace")
            self.logger.info(f"Library Retrieved ("
                             f"division={division}, "
                             f"name={library_name_by_division})")

            df_library_by_division = self._api.tfidf().read(library_name_by_division).df()
            if self._is_debug:
                self.logger.info('\n'.join([
                    f"Random Sample from Library (division={division})",
                    f"\tLibrary Name: {library_name_by_division}",
                    tabulate(df_library.sample(3), headers='keys', tablefmt='psql')]))

            df_update, terms_not_found = self.term_frequency(serial_number=serial_number,
                                                             df_cnum=df_cnum,
                                                             df_library=df_library_by_division)

            s = VectorSpaceTopNSelector(df=df_update,
                                        is_debug=self._is_debug)

            df_top = s.process(top_n=3,
                               expand=False,
                               key_field=serial_number)

            for _, row in df_top.iterrows():
                results.append({
                    "SerialNumber": serial_number,
                    "Division": division,
                    "Rank": row['Rank'],
                    "Term": row['Term']})

            for term in terms_not_found:
                results.append({
                    "SerialNumber": serial_number,
                    "Division": division,
                    "Rank": -1,
                    "Term": term})

        return pd.DataFrame(results)


def main():
    COLLECTION_NAME = 'supply_tag_20191025'
    serial_number = "9A3979897"
    df_final = CompareCnumAcrossDivisions(is_debug=False,
                                          collection_name=COLLECTION_NAME).process(serial_number)

    print(tabulate(df_final, headers='keys', tablefmt='psql'))


if __name__ == "__main__":
    main()
