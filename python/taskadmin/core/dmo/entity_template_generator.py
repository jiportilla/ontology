#!/usr/bin/env python
# -*- coding: UTF-8 -*-


from base import BaseObject
from base import LabelFormatter
from datadict import the_synonyms_dict


class EntityTemplateGenerator(BaseObject):
    """
    Generate a YAML Entity

    given the input
        entity = alpha beta
        parent = greek
        provenance = GTS (optional; default)

    generates the YAML entity
        Alpha Beta:
          type: Greek
          scoped: TRUE
          provenance:
            - GTS
          patterns:
            - 'alpha_beta'
            - 'alpha+beta'
    """

    def __init__(self):
        """
        Created:
            21-Mar-2019
            craig.trim@ibm.com
        """
        BaseObject.__init__(self, __name__)
        self.synonyms = the_synonyms_dict
        self.label_formatter = LabelFormatter()

    @staticmethod
    def _entity_p1(entity_label: str,
                   parent_label: str,
                   entity_pattern_1: str,
                   provenance: str) -> str:
        return """
{0}:
  type: {1}
  scoped: TRUE
  provenance:
    - {2}
  patterns:
    - '{3}'""".format(entity_label,
                      parent_label,
                      provenance,
                      entity_pattern_1)

    @staticmethod
    def _entity_p2(entity_label: str,
                   parent_label: str,
                   entity_pattern_1: str,
                   entity_pattern_2: str,
                   provenance: str) -> str:
        return """
{0}:
  type: {1}
  scoped: TRUE
  provenance:
    - {2}
  patterns:
    - '{3}'            
    - '{4}'""".format(entity_label,
                      parent_label,
                      provenance,
                      entity_pattern_1,
                      entity_pattern_2)

    @staticmethod
    def _entity_pattern_1(text: str):

        d_replace = {
            " ": "_",
            "/": "_",
            "!": "",
            ".": ""
        }

        text = text.lower()
        for k in d_replace:
            if k in text:
                text = text.replace(k, d_replace[k])

        text = text.strip()
        if not text or text.startswith("0") or (text.startswith("s") and len(text) == 5):
            return None
        return text

    def _find_canonical_term(self,
                             a_text: str) -> str:
        """ find canonical term """
        for k in self.synonyms:
            if k.lower() == a_text:
                return k
            for v in self.synonyms[k]:
                if v.lower() == a_text:
                    return k

        return a_text

    @staticmethod
    def _normalize_text(some_text: str):
        some_text = some_text.replace("Z/Os", "zos")
        some_text = some_text.replace("/", " ").replace("!", "")
        return some_text.lower().strip()

    def _entity_pattern_2(self,
                          text: str) -> str:
        invalid_patterns = ["for", "and", "-", "with", "of", "&"]

        def _is_valid(a_text: str) -> bool:
            return a_text.lower().strip() not in invalid_patterns

        tokens = self._normalize_text(text).split(" ")
        tokens = [self._find_canonical_term(x.lower().strip()) for x in tokens
                  if _is_valid(x)]
        tokens = sorted(set(tokens))

        total_spaces = len(text.split(" "))
        total_tokens = len(tokens)
        if (total_spaces - total_tokens) > 2:
            return self._entity_pattern_1(text)

        return "+".join(tokens).strip()

    def process(self,
                some_entity: str,
                some_parent: str,
                some_provenance="GTS"):

        entity_pattern_1 = self._entity_pattern_1(some_entity)
        entity_pattern_2 = self._entity_pattern_2(some_entity)
        if not entity_pattern_1:
            return None

        entity = self.label_formatter.process(some_entity)
        parent = self.label_formatter.process(some_parent)

        if entity_pattern_1 == entity_pattern_2:
            return self._entity_p1(entity_label=entity,
                                   parent_label=parent,
                                   entity_pattern_1=entity_pattern_1,
                                   provenance=some_provenance)

        return self._entity_p2(entity_label=entity,
                               parent_label=parent,
                               entity_pattern_1=entity_pattern_1,
                               entity_pattern_2=entity_pattern_2,
                               provenance=some_provenance)
