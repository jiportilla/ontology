# !/usr/bin/env python
# -*- coding: UTF-8 -*-


from base import BaseObject
from datadict.core.os import the_country_code_dict


class FindCountryCode(BaseObject):
    """ One-Stop-Shop Service API for Country Code queries """

    _d_country_codes = the_country_code_dict

    _l_known_unknowns = ["NAN", "NONE"]

    def __init__(self):
        """
        Created:
            14-May-2019
            craig.trim@ibm.com
            *   https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/254
        """
        BaseObject.__init__(self, __name__)

    def find_by_code(self,
                     some_code):
        some_code = some_code.upper().strip()
        if some_code in self._l_known_unknowns:
            return "unknown"

        if some_code not in self._d_country_codes:
            self.logger.warning(f"Unknown Country: code={some_code}")
            return "unknown"

        return self._d_country_codes[some_code].lower()
