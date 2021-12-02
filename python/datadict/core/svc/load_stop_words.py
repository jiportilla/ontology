# !/usr/bin/env python
# -*- coding: UTF-8 -*-


import codecs
import os

from base import BaseObject


class LoadStopWords(BaseObject):
    __cached = []

    def __init__(self,
                 is_debug: bool = False):
        """
        Created:
            23-Mar-2019
            craig.trim@ibm.com
        Updated:
            10-Aug-2019
            craig.trim@ibm.com
            *   added 'refresh-cache' param to 'load' method
            *   updated documentation
        """
        BaseObject.__init__(self, __name__)
        self.is_debug = is_debug

    @classmethod
    def learned(cls) -> list:
        """
        Purpose:
            a list of low discriminative terms typically via manual analysis
            in non-Cendant domains, these may have a great deal of value
        :return:
            a list of stopwords
        """
        path = os.path.join(os.environ["CODE_BASE"],
                            os.environ["STOP_WORDS_LEARNING"])
        target = codecs.open(path, "r", "utf-8")
        lines = [x.lower().strip() for x in target.readlines() if x]
        target.close()

        return lines

    @classmethod
    def high_frequency(cls) -> list:
        """
        Purpose:
            a list of low discriminative terms typically via TF-IDF analysis
            in non-Cendant domains, these may have a great deal of value
        :return:
            a list of stopwords
        """
        path = os.path.join(os.environ["CODE_BASE"],
                            os.environ["STOP_WORDS_HF"])
        target = codecs.open(path, "r", "utf-8")
        lines = [x.lower().strip() for x in target.readlines() if x]
        target.close()

        return lines

    @classmethod
    def standard(cls) -> list:
        """
        Purpose:
            "Standard" stopwords are generic stopwords
            These are non-controversial and provide little discriminative value in any domain
        :return:
            a list of stopwords
        """
        from datadict.core.os import the_stopwords_dict

        return the_stopwords_dict

    @classmethod
    def _from_file(cls,
                   an_instance) -> list:
        """
        :return:
            a list of Stopwords from the Filesystem
        """
        stopwords = sorted(set(cls.learned() +
                               cls.standard() +
                               cls.high_frequency()))
        if an_instance.is_debug:
            an_instance.logger.debug(f"File Retrieval "
                                     f"(total = {len(stopwords)})")
        cls.__cached = stopwords
        return stopwords

    def load(self,
             refresh_cache: bool = False) -> list:
        """
        Purpose:
            Load Stopwords
        :param refresh_cache:
            True        ignore the contents of the Redis cache
                        this is a useful form of cache invalidation during development
        :return:
            a list of stopwords
        """
        if not refresh_cache and len(self.__cached):
            return self.__cached

        return self._from_file(self)
