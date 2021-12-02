# !/usr/bin/env python
# -*- coding: UTF-8 -*-


import codecs
import os

from base import BaseObject


class LoadWordnet(BaseObject):

    def __init__(self):
        """
        Created:
            23-Mar-2019
            craig.trim@ibm.com
        """
        BaseObject.__init__(self, __name__)

    def load(self) -> list:
        """ load from file """
        path = os.path.join(os.environ["CODE_BASE"],
                            "resources/nlu/other/wordnet_kb.csv")
        target = codecs.open(path, "r", "utf-8")
        lines = [x.lower().strip() for x in target.readlines() if x]
        target.close()

        self.logger.debug("\n".join([
            "Load Wordnet Terms",
            "\tTotal: {}".format(len(lines))]))

        return sorted(set(lines))
