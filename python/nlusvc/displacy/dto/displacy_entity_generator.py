#!/usr/bin/env python
# -*- coding: UTF-8 -*-


class DisplacyEntityGenerator(object):

    @staticmethod
    def generate(text: str,
                 entity_type: str,
                 label: str,
                 start: int,
                 end: int):
        """
        Purpose:
            Generate a displacy entity for UI visualization
        :param text:
            Optional        used for traceability
        :param entity_type:
            Optional        used for traceability
        :param label:
            Mandatory       this is what displaCy will show on the UI
        :param start:
            Mandatory       this is the start of the highlighted span
        :param end:
            Mandatory       this is the end of the highlighted span
        :return:
        """
        return {
            "text": text,
            "type": entity_type,
            "start": start,
            "end": end,
            "label": label}
