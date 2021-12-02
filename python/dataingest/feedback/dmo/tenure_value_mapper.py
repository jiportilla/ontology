#!/usr/bin/env python
# -*- coding: UTF-8 -*-


from base import BaseObject


class TenureValueMapper(BaseObject):
    """ Perform a rollup from Countries to Regions based on an explicit mapping file """

    def __init__(self,
                 is_debug: bool = False):
        """
        Created:
            26-Nov-2019
            craig.trim@ibm.com
            *   refactored out of 'ingest-internal-feedback' while in pursuit of
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1449
        :param is_debug:
        """
        BaseObject.__init__(self, __name__)

        self._is_debug = is_debug

    def lookup(self,
               tenure: str) -> int:

        if type(tenure) != str:
            self.logger.warning(f"Unrecognized Tenure ("
                                f"value={tenure}, "
                                f"type={type(tenure)})")
            return 0

        tenure = tenure.lower().strip()
        if "<6" in tenure or "< 6" in tenure:
            return 1
        if "6mos" in tenure or "6 mos" in tenure:
            return 2
        if "1-2" in tenure:
            return 3
        if "3-5" in tenure:
            return 4
        if "6-10" in tenure:
            return 5
        if "11-15" in tenure:
            return 6
        if "16-20" in tenure:
            return 7
        if "21-25" in tenure:
            return 8
        if "26-30" in tenure:
            return 9
        if "31" in tenure:
            return 10

        self.logger.error(f"Unrecognized Tensure: {tenure}")
        raise NotImplementedError
