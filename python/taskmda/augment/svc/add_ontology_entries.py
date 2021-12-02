#!/usr/bin/env python
# -*- coding: UTF-8 -*-


import pprint
from typing import Optional

from base import BaseObject
from base import LabelFormatter
from datadict import LoadStopWords
from nlusvc import TextAPI


class AddOntologyEntries(BaseObject):
    """ Service to Augment the Cendant Ontology with new entries

        all output from this file represents net-new non-duplicated entries in the OWL file
    """

    def __init__(self,
                 is_debug: bool = True):
        """
        Created:
            25-Jul-2019
            craig.trim@ibm.com
            *   https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/493
        Updated:
            8-Jan-2020
            craig.trim@ibm.com
            *   add optional rdfs-definedby and version-info link
                https://github.ibm.com/-cdo/unstructured-analytics/issues/1703#issuecomment-17023405
        """
        BaseObject.__init__(self, __name__)

        self.is_debug = is_debug
        self._text_api = TextAPI(is_debug=False)
        self._stopwords = LoadStopWords(is_debug=False).load()

    @staticmethod
    def _validate(some_rel: str or list,
                  is_strict: bool = True):
        if not some_rel:
            return

        def validate_str(a_str_val: str):
            if not len(a_str_val.strip()):
                raise ValueError
            if '(' in a_str_val:
                raise ValueError(a_str_val)
            if '"' in a_str_val:
                raise ValueError(a_str_val)
            if is_strict and '-' in a_str_val:
                raise ValueError(a_str_val)

        if type(some_rel) == str:
            validate_str(str(some_rel))
        elif type(some_rel) == list:
            [validate_str(str(x)) for x in list(some_rel)]
        else:
            raise ValueError

    def _template(self,
                  some_id: str,
                  label: str,
                  comment: str,
                  sub_class: str,
                  part_of: Optional[list] = None,
                  implications: Optional[list] = None,
                  see_also: Optional[list] = None,
                  version_info: Optional[str] = None,
                  is_defined_by: Optional[str] = None) -> str:

        self._validate(some_id)
        self._validate(sub_class)
        self._validate(part_of)
        self._validate(implications)
        self._validate(see_also, is_strict=False)
        self._validate(version_info, is_strict=False)

        lines = []

        lines.append(f"###  http://www.semanticweb.org/craigtrim/ontologies/cendant#{some_id}")
        lines.append(f":{some_id} rdf:type owl:Class ;")
        lines.append(f"\trdfs:subClassOf :{sub_class} ;")

        if is_defined_by:
            lines.append(f"\trdfs:isDefinedBy \"{is_defined_by}\" ;")

        if version_info:
            lines.append(f"\towl:versionInfo \"{version_info}\" ;")

        if see_also:
            for a_see_also in see_also:
                lines.append(f"\trdfs:seeAlso \"{a_see_also}\" ;")

        if part_of:
            for part_of_rel in part_of:
                lines.append(f"\t:partOf :{part_of_rel} ;")

        if implications:
            for implies_rel in implications:
                lines.append(f"\t:implies :{implies_rel} ;")

        lines.append(f"\trdfs:comment \"{comment}\" ;")
        lines.append(f"\trdfs:label \"{label}\" .")

        return '\n'.join(lines)

    @staticmethod
    def _owl_id(a_term: str) -> str:

        if '_(' in a_term:
            a_term = a_term.replace('_(', ' ').replace(')', '')

        if '(' in a_term:
            a_term = a_term[:a_term.index('(')].strip()

        spacers = ['-', '–', '/']  # hyphens are not dupes
        for spacer in spacers:
            while spacer in a_term:
                a_term = a_term.replace(spacer, ' ')

        removals = ["'", ',', '"', '!', '?', '.', '.']
        for removal in removals:
            while removal in a_term:
                a_term = a_term.replace(removal, '')

        def camel_case(some_term: str) -> str:
            return LabelFormatter.camel_case(some_term,
                                             len_threshold=1,
                                             split_tokens=False,
                                             enforce_upper_case=False)

        return '_'.join([camel_case(x)
                         for x in a_term.split(' ')])

    def _sub_class_id(self,
                      a_sub_class) -> str:
        if not a_sub_class:
            return "Unknown"

        return self._owl_id(a_sub_class)

    @staticmethod
    def _cleanse_comment(a_comment: str) -> str:
        """
        Purpose:
            Cleanse a comment for OWL usage
        Sample Input:
            A polymer (; Greek poly-, "many" + -mer, "part") is a large molecule
        Sample Output:
            A polymer is a large molecule
        :param a_comment:
            any comment that may not meet OWL/TTL standards
        :return:
            an OWL/TTL compliant comment
        """

        a_comment = a_comment.replace('"', '')

        if '(' in a_comment and ')' in a_comment:
            x = a_comment.index('(')
            y = a_comment.index(')')
            a_comment = f"{a_comment[:x]}{a_comment[y + 1:]}"

        while '  ' in a_comment:
            a_comment = a_comment.replace('  ', ' ')

        return a_comment

    @staticmethod
    def _cleanse_term(a_term: str) -> str:
        if '(' in a_term and ')' in a_term:
            x = a_term.index('(')
            y = a_term.index(')')
            a_term = f"{a_term[:x]}{a_term[y + 1:]}".strip()

        if a_term.endswith('_'):
            a_term = a_term[:len(a_term) - 1]

        return a_term.strip()

    def _cleanse_rels(self,
                      a_rel: Optional[list]) -> list:
        if not a_rel or not len(a_rel):
            return []
        return [self._owl_id(x) for x in a_rel]

    @staticmethod
    def _cleanse_seealso(see_also: Optional[list]) -> list:
        if not see_also:
            return []

        see_also = [x for x in see_also if len(x) > 2]

        def cleanse(a_term: str) -> str:
            if '(' in a_term:
                a_term = a_term[:a_term.index('(')]

            a_term = a_term.replace('"', '')
            return a_term

        see_also = [cleanse(x) for x in see_also]

        return see_also

    def process(self,
                terms: list,
                sub_class: str,
                part_of: Optional[list],
                see_also: Optional[list],
                is_defined_by: Optional[str],
                version_info: Optional[str],
                implications: Optional[str],
                comment: str) -> list:

        part_of = self._cleanse_rels(part_of)
        comment = self._cleanse_comment(comment)
        sub_class = self._sub_class_id(sub_class)
        see_also = self._cleanse_seealso(see_also)
        implications = self._cleanse_rels(implications)

        entities = []
        for term in terms:

            owl_id = self._owl_id(term)
            term = self._cleanse_term(term)
            label = LabelFormatter.camel_case(term, split_tokens=True)

            try:
                entities.append(self._template(label=label,
                                               comment=comment,
                                               see_also=see_also,
                                               part_of=part_of,
                                               implications=implications,
                                               some_id=owl_id,
                                               sub_class=sub_class,
                                               version_info=version_info,
                                               is_defined_by=is_defined_by))
            except ValueError as e:
                self.logger.error('\n'.join([
                    "OWL Entity Generation Failed",
                    f"\tID: {owl_id}",
                    f"\tSubClass: {sub_class}",
                    f"\tPartonomy: {part_of}",
                    f"\tImplications: {implications}",
                    f"\tSeeAlso: {see_also}",
                    f"\tCommenht: {comment}"]))
                self.logger.exception(e)
                raise ValueError

        entities = sorted(entities)
        if self.is_debug:
            self.logger.debug('\n'.join([
                "Generated Ontology Entities",
                f"\tInput Size: {len(entities)}",
                pprint.pformat(entities, indent=4)]))

        return entities
