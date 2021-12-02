#!/usr/bin/env python
# -*- coding: UTF-8 -*-


from collections import Counter

from base import BaseObject
from nlutext import NormalizeIncomingText


class EducationFieldCounter(BaseObject):
    """
    """

    _cache = {}

    def __init__(self,
                 records: list,
                 field_name: str,
                 is_debug: bool = False):
        """
        Created:
            17-Oct-2019
            craig.trim@ibm.com
            *   refactored out of perform-education-analysis
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1142
        Updated:
            30-Oct-2019
            craig.trim@ibm.com
            *   fix defect in data type iteration in fields
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1151#issuecomment-15650388
        :param records:
            the records to be counted
        :param field_name:
            the field that was originally queried by and that has yet to be filtered by
            e.g., 'major' or 'degree'
        :param is_debug:
        """
        BaseObject.__init__(self, __name__)
        from nlusvc.core.bp import TextAPI

        self._records = records
        self._is_debug = is_debug
        self._field_name = field_name
        self._text_api = TextAPI(is_debug=False)
        self._normalizer = NormalizeIncomingText(is_debug=False)

    def _preprocess(self,
                    value: str) -> str:
        return self._text_api.preprocess(input_text=value,
                                         lowercase=True,
                                         no_urls=True,
                                         no_emails=True,
                                         no_phone_numbers=True,
                                         no_numbers=True,
                                         no_currency_symbols=True,
                                         no_punct=True,
                                         no_contractions=True,
                                         no_accents=True)

    def _normalize(self,
                   value: str) -> str:
        return self._normalizer.process(value)["normalized"]

    def process(self,
                preprocess: bool,
                normalize: bool,
                log_threshold: int = 10) -> Counter:
        """
        Purpose:
            Create a Value Counter by Field Name
        Sample Output:
        :param preprocess:
            True            perform textacy preprocessing
        :param normalize:
            True            perform Cendant normalization
        :param log_threshold:
            <N, Optional>   feedback on progress at this interval
        :return:
        """
        ctr = 0
        c = Counter()

        records = self._records
        total_records = len(records)

        for record in records:

            ctr += 1
            if self._is_debug and log_threshold and ctr % log_threshold == 0:
                self.logger.debug(f"Progress {ctr} - {total_records}")

            fields = [field for field in record["fields"]
                      if field["name"] == self._field_name]

            for field in fields:

                def value_or_unknown() -> str:
                    value = field["normalized"]
                    vtype = type(field['normalized'])
                    if vtype == list:  # 1151#issuecomment-15650388
                        if not len(value):
                            return 'unknown'
                        if len(value) == 1 and not len(value[0]):
                            return 'unknown'
                        return ' '.join(value)
                    elif vtype == str:
                        if not value or not len(value.strip()):
                            return 'unknown'
                    return value

                original = value_or_unknown().lower().strip()

                def normalize_value() -> str:
                    if not preprocess and not normalize:
                        return original
                    elif original in self._cache:  # retrieve from cache
                        return self._cache[original]
                    normalized = original
                    if preprocess:
                        normalized = self._preprocess(normalized)
                    if normalize:
                        normalized = self._normalize(normalized)
                    self._cache[original] = normalized
                    return normalized

                c.update({normalize_value(): 1})

        return c
