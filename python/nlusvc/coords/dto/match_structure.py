#!/usr/bin/env python
# -*- coding: UTF-8 -*-


class MatchStructure(object):

    @staticmethod
    def generate(input_string: str,
                 entity_text: str or list,
                 match_text: str,
                 x: int, y: int) -> dict:
        """
        :param input_string:
            the input string that is being searched
        :param entity_text:
            the (Ontology) entity in the string
        :param match_text:
            the extracted segment of the input string corresponding to the Ontology entity
        :param x:
            the start (x) position of the match string
        :param y:
            the end (y) position of the match string
        :return:
            a Match Structure (dict)
        """
        if type(entity_text) == list:
            entity_text = ' '.join(entity_text)

        return {
            "input": input_string,
            "entity": entity_text,
            "x": x, "y": y,
            "substring": match_text}
