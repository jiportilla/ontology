#!/usr/bin/env python
# -*- coding: UTF-8 -*-


from base import BaseObject
from datadict import FindCertifications


class CertificationConfidenceComputer(BaseObject):
    """ Lower the Confidence of Certification Tags when
        the 'Certification' Keyword does not exist in the Original UPS """

    __score_penalty = 35

    __finder = FindCertifications()

    def __init__(self,
                 original_text: str,
                 normalized_text: str,
                 tag_tuples: list,
                 is_debug: bool = False):
        """
        Created:
            6-Sept-2019
            craig.trim@ibm.com
            *   https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/878
        Updated:
            10-Sept-2019
            craig.trim@ibm.com
            *   add both original-ups and normalized-ups to param list for debugging purposes
        Updated:
            12-Sept-2019
            craig.trim@ibm.com
            *   fix indentation defect per
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/924
        Updated:
            13-Sept-2019
            craig.trim@ibm.com
            *   has-mention keyword must use original text
        :param tag_tuples:
            a list of tag tuples
            Sample Input:
                [   ('Java Certification', 90),
                    ('Data Scientist', 95),
                    ...
                    ('AWS Certification', 80)
                ]
        """
        BaseObject.__init__(self, __name__)
        self._is_debug = is_debug
        self._tag_tuples = tag_tuples
        self._original_text = original_text
        self._normalized_text = normalized_text

    def _has_mention(self) -> bool:
        """
        Purpose:
            False Positive Reduction via Keyword Examination in Original UPS
        Notes:
            -   this has to be run on the Original Text
                the normalized text will impose detail via assumption and inference that the user did not type
        Example:
            -   Metlife has the GSSP platform (Global Sales & Servicing)
                and so a consultant may list 'Metlife GSSP' on their CV
            -   the 'GSSP' keyword is also a certification (GIAC Secure Software Programmer)
        Resolution:
            -   if a user does not have the word 'certification' in the text, the confidence in this texdt
                as a certification should be reduced by N
        :return:
            True    if and only if the 'Certification' keyword exists in the Input Text
        """
        terms = ["certified", "certification"]

        if "not_certified" in self._original_text.lower():
            return False

        for term in terms:
            if term in self._original_text.lower():
                return True

        return False

    def _update_tag_tuples(self) -> list:
        """
        Purpose:
            Lower the Confidence of any Certification Tag
        IMPORTANT:
            -   This function assumes that the 'Certification' keyword
                does not exist in the Original UPS
            -   This assumption implies that this condition was fulfilled
                in a prior step
        :return:
            an updated list of tag-tuples
        """
        _updated = []
        for tag_tuple in self._tag_tuples:

            # Step: Not a Certification; carry on
            if not self.__finder.exists(tag_tuple[0]):
                _updated.append(tag_tuple)

            else:

                # Step: Lower the Score
                score = round(tag_tuple[1] - self.__score_penalty, 2)
                if score < 0:
                    score = 0
                elif score > 100:
                    score = 100

                # Step: Create a new Tuple
                _updated.append((tag_tuple[0], score))

                self.logger.debug('\n'.join([
                    f"Lowered Certification Confidence:",
                    f"\tCertification: {tag_tuple[0]}",
                    f"\tOriginal Score: {tag_tuple[1]}",
                    f"\tUpdated Score: {score}",
                    f"\tOriginal UPS: {self._original_text}",
                    f"\tNormalized UPS: {self._normalized_text}"]))

        return _updated

    def process(self) -> list:
        is_mentioned = self._has_mention()

        if is_mentioned:
            # Step: The 'Certification' keyword exists
            #       No need to run this function further
            return self._tag_tuples

        # Step: The 'Certification' keyword does not exist
        #       Lower the Confidence of Certification tags (if any)
        return self._update_tag_tuples()
