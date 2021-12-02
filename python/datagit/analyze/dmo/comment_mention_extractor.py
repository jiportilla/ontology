# !/usr/bin/env python
# -*- coding: UTF-8 -*-


from base import BaseObject
from datamongo import CendantRecordParser
from nlutext import PunctuationRemover


class CommentMentionExtractor(BaseObject):
    """ Extract Mentions from Long Text
        Sample Input:
            this text is for @john-doe
        Sample Output:
            [john-doe]
    """

    __mentions_blacklist = ['staticmethod', 'n', 'here', '5pm', '11pm', 'channel', 'generated', 'cancel']

    def __init__(self,
                 d_record: dict,
                 is_debug: bool = True):
        """
        Created:
            31-Dec-2019
            craig.trim@ibm.com
            *   refactored out of 'comment-node-builder'
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1681#issuecomment-16873584
            *   add cendant-record-parser
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1681#issuecomment-16873873
        :param is_debug:
            if True     increase log output at DEBUG level
        """
        BaseObject.__init__(self, __name__)
        self._is_debug = is_debug
        self._d_record = d_record

    @staticmethod
    def _cleanse(a_mention: str) -> str:
        token = PunctuationRemover(is_debug=False,
                                   the_input_text=a_mention).process().strip()
        if ' ' in token:  # take first token only
            token = token.split(' ')[0].strip()
        if str(token[-1:]) == '1':  # remove trailing digits
            token = token[:len(token) - 1]
        return token

    def _extract(self) -> list:
        parser = CendantRecordParser(is_debug=self._is_debug)
        body = parser.field_value_by_name(a_record=self._d_record,
                                          a_field_name='Body')
        return [token[1:] for token in body.split(' ')
                if token.startswith('@')]

    def process(self) -> list:
        mentions = self._extract()
        mentions = [mention.strip() for mention in mentions]
        mentions = [mention for mention in mentions if mention not in self.__mentions_blacklist]
        mentions = [mention for mention in mentions if mention and len(mention)]
        mentions = [mention for mention in mentions if ':' not in mention]
        mentions = [self._cleanse(mention) for mention in mentions]
        mentions = [mention for mention in mentions if mention and len(mention)]
        mentions = [mention for mention in mentions if mention not in self.__mentions_blacklist]
        mentions = sorted(mentions)

        if self._is_debug and len(mentions):
            self.logger.debug('\n'.join([
                "Extracted Mentions",
                f"\tNames: {mentions}"]))

        return mentions
