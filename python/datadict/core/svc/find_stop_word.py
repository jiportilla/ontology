# !/usr/bin/env python
# -*- coding: UTF-8 -*-


from base import BaseObject


class FindStopword(BaseObject):
    """ act as a controller in front of all stop word """

    def __init__(self, some_token):
        """
        Created:
            19-Apr-2017
            craig.trim@ibm.com
        Updated:
            21-Feb-2019
            craig.trim@ibm.com
            *   migrated to text
        """
        BaseObject.__init__(self, __name__)
        self.token = some_token

    @staticmethod
    def find_stopwords():
        from datadict import the_stopwords_dict

        return the_stopwords_dict

    def exists(self):
        from datadict import the_stopwords_dict

        return self.token.lower() in the_stopwords_dict
