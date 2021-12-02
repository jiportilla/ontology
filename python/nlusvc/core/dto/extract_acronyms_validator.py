#!/usr/bin/env python
# -*- coding: UTF-8 -*-


from base import BaseObject
from base import MandatoryParamError


class ExtractAcronymsValidator(BaseObject):

    def __init__(self,
                 source_text: str,
                 include_acronyms: bool,
                 include_instances: bool):
        """
        Created:
            27-Jun-2019
            craig.trim@ibm.com
            *   designed as a validation method for the 'extract-acronyms' service
        """
        BaseObject.__init__(self, __name__)

        if type(include_acronyms) != bool:
            raise MandatoryParamError("Param 'include_acronyms' must be either 'True' or 'False'")

        if type(include_instances) != bool:
            raise MandatoryParamError("Param 'include_instances' must be either 'True' or 'False'")

        if not source_text or type(source_text) != str:
            raise MandatoryParamError("Source Text")

        if not include_acronyms and not include_instances:
            raise MandatoryParamError("Must Include either Acronyms or Instances")
