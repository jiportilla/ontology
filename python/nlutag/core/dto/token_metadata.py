#!/usr/bin/env python
# -*- coding: UTF-8 -*-

import logging

logger = logging.getLogger(__name__)


class TokenMetadata(object):
    def __init__(self, some_matches):
        """
        Updated:
            21-Feb-2017
            craig.trim@ibm.com
            *   modified process to remove boolean flags
        :param some_matches:
        """
        self.matches = some_matches

    @staticmethod
    def create_product_entity(some_label, some_type, is_negated, is_primary, is_scoped, is_spanned, confidence):
        return {
            "label": some_label,
            "type": some_type,
            "negated": is_negated,
            "primary": is_primary,
            "scoped": is_scoped,
            "spanned": is_spanned,
            "confidence": confidence}

    @staticmethod
    def create_empty_product_entity():
        return {
            "label": None,
            "type": None,
            "negated": False,
            "primary": False,
            "scoped": False,
            "spanned": False,
            "confidence": 100}

    @staticmethod
    def get_confidence(some_current_confidence, is_negated, is_primary, is_scoped, is_spanned):
        confidence = some_current_confidence

        if is_negated:
            confidence -= 10
        if not is_primary:
            confidence -= 10
        if not is_scoped:
            confidence -= 10
        if is_spanned:
            confidence -= 35

        if confidence < 0:
            return 0
        return confidence

    @staticmethod
    def get_current_max_confidence(the_entity):
        max_confidence = 0
        for a_match in the_entity["matches"]:
            if a_match["confidence"] > max_confidence:
                max_confidence = a_match["confidence"]

        return max_confidence
