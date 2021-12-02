#!/usr/bin/env python
# -*- coding: UTF-8 -*-


import os

from base import BaseObject


class ListVectorSpaceLibraries(BaseObject):
    """ Finds Vectorspace Libraries """

    def __init__(self,
                 is_debug: bool = False):
        """
        Created:
            5-Nov-2019
            craig.trim@ibm.com
            *   https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1261#issuecomment-15773882
        """
        BaseObject.__init__(self, __name__)
        self._is_debug = is_debug

    def _libraries(self):
        fpath = os.path.join(os.environ['GTS_BASE'],
                             'resources/confidential_input/vectorspace')

        libraries = []
        for root, dirs, files in os.walk(fpath):
            for filename in files:
                libraries.append(filename)

        if self._is_debug:
            self.logger.debug('\n'.join([
                f"Located Libraries (total={len(libraries)})",
                f"\tPath: {fpath}",
                f"\tLibaries: {libraries}"]))

        return libraries

    def _match_library_type(self,
                            a_library: str,
                            division: str,
                            library_type: str) -> bool:
        """
        Purpose:
            Match a Library to a Library type
        Library Types and Naming Standards:
            'vectorspace'       SUPPLY_TAG_20191025_TFIDF.csv
            'inverted'          SUPPLY_TAG_20191025_TFIDF_INVERTED.csv
        :param a_library:
            a given library
        :param division:
            a division
        :param library_type:
            a given library type
            e.g., 'vectorspace' or 'inverted'
        :return:
            True        the given library matches the given library type
            False       no match exists
        """
        if division and division not in a_library:
            return False

        if library_type == "inverted":
            return library_type in a_library
        elif library_type == "vectorspace":
            return "inverted" not in a_library
        raise NotImplementedError

    def process(self,
                library_type: str,
                division: str = None,
                latest_only: bool = True) -> list or str:
        """
        Purpose:
            Find Libraries to use in Vectorspace Calculations
        :rtype: object
        :param library_type:
            a given library type
            e.g., 'vectorspace' or 'inverted'
        :param division:
            str         some libraries are created by division
                        if the division is specified, find the corresponding library
            None        non-division specific libraries only
        :param latest_only:
            True        only return a single match as a string
                        this will be the most recent (by date)
            False       return all matches as a list of strings
        :return:
            str         the most recent match by date
            list        all matches
        """

        def _library_type() -> str:
            return library_type.lower()

        def _division() -> str or None:
            if division:
                return division.lower()

        def inner_process() -> list or str:
            libraries = [x for x in self._libraries()
                         if self._match_library_type(a_library=x.lower(),
                                                     division=_division(),
                                                     library_type=_library_type())]

            if not len(libraries):
                if division:
                    self.logger.warning(f"Library By Division Not Found "
                                        f"(division={division})")
                return None
            if latest_only:
                return sorted(libraries)[-1]

            return sorted(libraries)

        svcresult = inner_process()

        def _log_str(message: str) -> str:
            return '\n'.join([
                message,
                f"\tDivision: {division}",
                f"\tResults: {svcresult}",
                f"\tLatest Only? {latest_only}",
                f"\tLibrary Type: {library_type}"])

        if not svcresult:
            self.logger.warning(_log_str("No Library Found"))
        elif svcresult and self._is_debug:
            self.logger.warning(_log_str("Library List Results"))

        return svcresult
