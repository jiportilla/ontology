#!/usr/bin/env python
# -*- coding: UTF-8 -*-


import sys

from tabulate import tabulate

from cendalytics.tfidf.core.bp import VectorSpaceAPI
from datamongo import BaseMongoClient

IS_DEBUG = True

api = VectorSpaceAPI(is_debug=IS_DEBUG)


def create(collection_name: str,
           limit: int,
           division: str or None):
    """
    Purpose:
        Create the Vector Space
    :param collection_name:
        the name of the tag collection to build the vector space across
    :param limit:
        Optional    the number of records to return
    :param division:
        Optional    the division to limit the queries too
    """
    api.tfidf().create(limit=limit,
                       division=division,
                       collection_name=collection_name,
                       mongo_client=BaseMongoClient()).process()


def read(library_name: str,
         key_field: str):
    """
    Purpose:
        Read the Vector Space
    :param library_name:
        the VectorSpace library name to invert
    :param key_field:
        the key field to search the Vector Space on
    """
    if library_name.lower() == "latest":
        library_name = None  # convention; this will force latest

    reader = api.tfidf().read(library_name=library_name)
    df_results = reader.process(top_n=3,
                                expand=False,
                                key_field=key_field)
    print(tabulate(df_results, tablefmt='psql', headers='keys'))


def read_by_division(division: str,
                     key_field: str):
    """
    Purpose:
        Read the Vector Space
    :param division:
    :param key_field:
        the key field to search the Vector Space on
    """
    try:
        reader = api.read_by_division(division=division)
        df_results = reader.process(top_n=3,
                                    expand=False,
                                    key_field=key_field)
        print(tabulate(df_results, tablefmt='psql', headers='keys'))
    except ValueError as err:
        print(f"Read By Division Failed (reason={err})")


def invert(library_name: str,
           top_n: int):
    """
    Purpose:
        Perform Inversion Action
    :param library_name:
        the VectorSpace library name to invert
    :param top_n:
        the number of terms to retrieve from the VectorSpace for the inversion task
    """
    api.inversion().create(library_name=library_name).process(top_n=top_n)


def read_inversion(library_name: str,
                   term: str):
    """
    Purpose:
        Read an Inversion Library and find
            the key fields that correspond to the given term
    :param library_name:
        the Inversion library name to read
    :param term:
        the term to search on
    """
    if library_name.lower() == "latest":
        library_name = None  # convention; this will force latest

    results = api.inversion().read(library_name=library_name).process(term=term)
    print(results)


def list_libraries(library_type: str,
                   latest_only: bool,
                   division: str = None):
    """
    Purpose:
        List the Libraries that exist
    :param library_type:
        the type of library to list
        e.g., 'vectorspace' or 'inversion'
    :param division:
        the division
    :param latest_only:
        True        show the latest library only as a string
        False       show all libraries as a list
    """
    results = api.list().process(division=division,
                                 latest_only=latest_only,
                                 library_type=library_type)
    print(results)


def main(action, param2, param3, param4):
    """

    :param action:
        the action to perform
    :param param2:
        varies by action
    :param param3:
        varies by action
    :param param4:
        varies by action
    """

    def create_by_division():
        def _collection_name() -> str:
            return str(param2)

        from cendalytics.tfidf.consumer.recipe import CreateAndInvertAll
        CreateAndInvertAll(collection_name=_collection_name()).process()

    if action == "create":
        def _collection_name() -> str:
            return str(param2)

        def _limit() -> int:
            if type(param3) == str:
                return sys.maxsize
            return int(param3)

        def _division() -> str or None:
            if param4:
                return str(param4)

        create(collection_name=_collection_name(),
               limit=_limit(),
               division=_division())

    elif action == "create_by_division":
        create_by_division()

    elif action == "read":
        path = str(param2)
        key_field = str(param3)

        read(path, key_field)

    elif action == "read_by_division":
        def _division() -> str:
            return str(param2)

        def _key_field() -> str:
            return str(param3)

        read_by_division(division=_division(),
                         key_field=_key_field())


    elif action == "create_inversion":
        library_name = str(param2)
        top_n = int(param3)

        invert(library_name, top_n)

    elif action == "read_inversion":
        library_name = str(param2)
        term = str(param3)

        read_inversion(library_name, term)

    elif action == "list":
        def _library_type() -> str:
            return str(param2)

        def _latest_only() -> bool:
            return bool(param3)

        def _division() -> str or None:
            if param4:
                return str(param4)

        list_libraries(library_type=_library_type(),
                       latest_only=_latest_only(),
                       division=_division())


if __name__ == "__main__":
    import plac

    plac.call(main)
