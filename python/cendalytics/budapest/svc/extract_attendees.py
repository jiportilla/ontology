# !/usr/bin/env python
# -*- coding: UTF-8 -*-


import os

from base import BaseObject
from base import FileIO
from cendalytics.budapest.dmo import DataReaderForConference
from cendalytics.budapest.dmo import CvAssemblerByCnum


class ExtractAttendees(BaseObject):
    """
    """

    def __init__(self):
        """
        Created:
            4-Apr-2019
            craig.trim@ibm.com
        """
        BaseObject.__init__(self, __name__)

    @staticmethod
    def path():
        return os.path.join(os.environ["DESKTOP"],
                            "assembled-cvs.json")

    def process(self):
        df = DataReaderForConference().process()
        cvs = CvAssemblerByCnum(df).process()

        FileIO.json_to_file(self.path(), cvs)


if __name__ == "__main__":
    ExtractAttendees().process()
