#!/usr/bin/env python
# -*- coding: UTF-8 -*-
# pylint:disable=invalid-name

import sys
import time

from base import BaseObject
from base import MandatoryParamError
from datadict import FindSynonym

REPORT_TO_CONSOLE = hasattr(sys.modules['__main__'], '__file__') and \
                    'parse_single_record' in sys.modules['__main__'].__file__


def report_changes(func):
    """Decorator to helps us when using ./admin.sh task parse..."""

    def fast_warpper(*args):
        return func(*args)

    def reporting_wrapper(*args):
        self = args[0]
        before = args[1]
        after = func(*args)
        if after != before:
            level = 'regexp'
            if len(args) > 2:
                level = f'level: {args[2]}'
            print(f'Swapped synonyms: [{before}]->[{after}] ({level})')
        return after

    if REPORT_TO_CONSOLE:
        return reporting_wrapper
    else:
        return fast_warpper


class SynonymSwapper(BaseObject):
    """ Swap Synonyms """

    _synonyms_finder = None

    def __init__(self,
                 some_input: str,
                 some_iterations: int = 3,
                 slow_log_threshold: int = 25,
                 ontology_name: str = 'base',
                 is_debug: bool = False,
                 final_logger: bool = False):
        """
        Updated:
            25-Jan-2017
            craig.trim@ibm.com
            *   added swap / swap_by_level
        Updated:
            12-Feb-2017
            craig.trim@ibm.com
            *   changed final log statement to DEBUG
            *   added "some_iterations" param
            *   refactored 'MAX_NGRAM_LEVEL' into a global var
                formerly was a class-level variable
            *   multiple executions are required due to how synonyms file is created:
                locating => locate => find
                e.g.
                    "locating" is a synonym of "locate"
                    "locate" is a synonym of "find"
                an intermediate synonym file should be created
        Updated:
            21-Feb-2017
            *   added early break to iteration loop
        Updated:
            12-Apr-2017
            craig.trim@ibm.com
            *   added the function 'key_exists'
        Updated:
            13-Apr-2017
            craig.trim@ibm.com
            *   replaced the reg-exp in 'key_exists'
                with a sub-stringing algorithm (this is much quicker)
        Updated:
            19-Apr-2017
            craig.trim@ibm.com
            *   renamed from 'PerformSynonymSwapping'
        Updated:
            22-Apr-2017
            craig.trim@ibm.com
            *   added 'is_regexp' condition
        Updated:
            4-Jan-2018
            craig.trim@ibm.com
            *   modification noted in swap_by_level
                https://gain-jira-prd2.gain.sl.edst.ibm.com:8443/browse/AGP-1852
        Updated:
            21-Feb-2019
            craig.trim@ibm.com
            *   migrated to text
        Updated:
            21-Mar-2019
            craig.trim@ibm.com
            *   added 'implications' message for 'COND-2'
        Updated:
            11-April-2019
            anassar@us.ibm.com
            *   use reverse synonym dictionary for performance enhancement
        Updated:
            29-May-2019
            craig.trim@ibm.com
            *   experiment with redis
                (removed on 5-Jun-2019)
        Updated:
            29-Jul-2019
            craig.trim@ibm.com
            *   added 'slow-log-threshold' parameter
            *   attempt gross over-simplication of synonym swapping
                eliminating regex for massive speed increase in swap-by-level
        Updated:
            24-Oct-2019
            xavier.verges@es.ibm.com
            *   only iterate on the terms that have synonyms with the specific word length
            *   treat regexps in their own loop
        Updated:
            07-Nov-2019
            xavier.verges@es.ibm.com
            *   added the report_changes decorator
        Updated:
            26-Nov-2019
            xavier.verges@es.ibm.com
            *   remove extra blanks with while loop, faster than regexp
        Updated:
            13-Dec-2019
            craig.trim@ibm.com
            *   load dictionaries by ontology name
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1582
        :param some_input:
        :param some_iterations: the number of times to execute the synonym swap
        """
        BaseObject.__init__(self, __name__)
        if not some_input:
            raise MandatoryParamError("Input")

        self.input = some_input
        self.is_debug = is_debug
        self.final_logger = final_logger
        self.iterations = some_iterations
        self._slow_log_threshold = slow_log_threshold

        self._synonyms_finder = FindSynonym(is_debug=is_debug,
                                            ontology_name=ontology_name)

    @report_changes
    def swap_by_regexp(self,
                       some_normalized: str) -> str:
        original = some_normalized
        for regexp, replacement in self._synonyms_finder.regexps_with_synonyms().items():
            some_normalized = regexp.sub(replacement, some_normalized)
        if original != some_normalized:
            while "  " in some_normalized:
                some_normalized = some_normalized.replace("  ", " ")
        return some_normalized

    @report_changes
    def swap_by_level(self,
                      some_normalized: str,
                      some_level: int) -> str:
        """
        :param some_level:
            the n-gram level to perform swapping on
        :param some_normalized:
        :return:
            normalized string
        """

        for replacement in self._synonyms_finder.keys_in_swap_level(some_level):

            candidates = self._synonyms_finder.synonyms_in_swap_level(replacement, some_level)

            for candidate in candidates:
                if not candidate in some_normalized:
                    # we cannot trust a check done when pulling candidates,
                    # because some_normalized may have changed
                    continue
                # We may have a match, or our candidate may be just a particle
                # e.g.
                #   candidate: cert
                #   some_normalized: certification
                #   -> we should not match
                if some_normalized == candidate:
                    some_normalized = replacement
                else:
                    # % formatting seems to have better performance than f-string formatting
                    v1 = " %s " % candidate
                    if v1 in some_normalized:
                        some_normalized = some_normalized.replace(v1, " %s " % replacement)
                    else:
                        v2 = "%s " % candidate
                        if some_normalized.startswith(v2):
                            some_normalized = some_normalized.replace(v2, "%s " % replacement)
                        else:
                            v3 = " %s" % candidate
                            if some_normalized.endswith(v3):
                                some_normalized = some_normalized.replace(v3, " %s" % replacement)
        return some_normalized

    def swap(self, normalized):
        """
        Purpose:
            Create a "normalized string"
            - all synonyms replaced with canonical term from dictionary
            - start with high level n-grams and work down to unigrams

            Example:
                access~get to
                receive~get

                given
                    UPS  = "can not get to server"
                    nUPS = "can not access server"

                    UPS  = "I did not get the document"
                    nUPS = "I did not receive the document"

                if we didn't swap at highest n-gram levels first we could end up with

                    given
                        UPS  = "can not get to server"
                        nUPS = "can not receive to server"

        :return: normalized string
        """
        normalized = self.swap_by_regexp(normalized)
        level = self._synonyms_finder.max_ngram()
        while level > 0:
            normalized = self.swap_by_level(normalized, level)
            level -= 1
        return normalized

    def process(self):
        counter = 0
        start = time.time()

        normalized = self.input
        last_normalized = self.input

        while counter < self.iterations:
            normalized = self.swap(normalized).strip()

            # check if the synonym swapping algorithm modified the string
            if normalized == last_normalized:
                # no changes were made
                break

            last_normalized = normalized
            counter += 1

        end = int(time.time() - start)
        if end > self._slow_log_threshold:
            self.logger.warning('\n'.join([
                f"Slow Swapping: {end}s, "
                f"input={len(self.input)}, "
                f"output={len(normalized)}"]))

        if self.final_logger and self.input != normalized:
            self.logger.debug(f"Synonym Swapping Complete (time={end}s)")

        return normalized
