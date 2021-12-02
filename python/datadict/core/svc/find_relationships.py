# !/usr/bin/env python
# -*- coding: UTF-8 -*-


from base import BaseObject
from base import MandatoryParamError


class FindRelationships(BaseObject):
    """ a single API for finding any relationship of any type across the knowledge base """

    def __init__(self,
                 ontology_name: str = 'base',
                 is_debug: bool = False):
        """
        Created:
            26-Mar-2019
            craig.trim@ibm.com
            *   based on 'find-ontology-entity'
        Updated:
            24-Apr-2019
            craig.trim@ibm.com
            *   added 'selector'
        Updated:
            30-May-2019
            craig.trim@ibm.com
            *   update documentation and add 'is_*' and 'has_' methods
        Updated:
            24-Sept-2019
            craig.trim@ibm.com
            *   added 'has-ancestor' function
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/992
        Updated:
            30-Sept-2019
            craig.trim@ibm.com
            *   added 'all-ancestors' function
        Updated:
            25-Nov-2019
            craig.trim@ibm.com
            *   update see-also method
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1442
        Updated:
            13-Dec-2019
            craig.trim@ibm.com
            *   load dictionaries by ontology name
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1582
        """
        BaseObject.__init__(self, __name__)
        from datadict import FindEntity
        from datadict.core.os import the_references_dict
        from datadict.core.dmo import DictionaryLoader

        self.d_cache = {}
        self.entity_finder = FindEntity(is_debug=is_debug,
                                        ontology_name=ontology_name)

        self._loader = DictionaryLoader(is_debug=is_debug,
                                        ontology_name=ontology_name)

        self.d_references = the_references_dict
        self.d_parents = self._loader.taxonomy().parents()
        self.d_see_also = self._loader.synonyms().seeAlso()
        self.d_owns = self._loader.relationships().owns()
        self.d_parts = self._loader.relationships().parts()
        self.d_implies = self._loader.relationships().implies()
        self.d_defines = self._loader.relationships().defines()
        self.d_requires = self._loader.relationships().requires()
        self.d_versions = self._loader.relationships().versions()
        self.d_runson = self._loader.relationships().runsOn()
        self.d_produces = self._loader.relationships().produces()
        self.d_similarity = self._loader.relationships().similarity()
        self.d_infinitive = self._loader.relationships().infinitive()

    @staticmethod
    def _cleanse(some_token: str) -> str:
        return some_token.lower().strip()

    def _forward_relationship(self,
                              some_token: str,
                              some_dict: dict) -> list:
        some_token = self._cleanse(some_token)
        for key in some_dict:
            if self._cleanse(key) == some_token:
                return some_dict[key]
        return []

    def _reverse_relationship(self,
                              some_token: str,
                              some_dict: dict) -> list:
        s = set()
        some_token = self._cleanse(some_token)
        for key in some_dict:
            for value in some_dict[key]:
                if self._cleanse(value) == some_token:
                    s.add(key)
        return sorted(s)

    @staticmethod
    def _all_relationships(some_dict: dict,
                           bidirectional: bool = False) -> dict:

        if type(some_dict) != dict:
            raise MandatoryParamError("\n".join([
                "Incorrect Datatype Parameter",
                "\tExpected: dict",
                "\tActual: {} (value={})".format(
                    type(some_dict), some_dict)]))

        d_rels = {}
        for k in some_dict:
            for v in some_dict[k]:
                if k.lower().strip() == v.lower().strip():
                    continue

                if v not in d_rels:
                    d_rels[v] = set()
                d_rels[v].add(k)

                if bidirectional:
                    if k not in d_rels:
                        d_rels[k] = set()
                    d_rels[k].add(v)

        return d_rels

    def all_relationships(self,
                          token: str):
        return self.selector(token=token,
                             rel_partonomy=True,
                             rel_requires=True,
                             rel_similarity=True,
                             rel_owns=True,
                             rel_taxonomy=True,
                             rel_version=True,
                             rel_implies=True)

    def selector(self,
                 token: str,
                 rel_partonomy: bool,
                 rel_requires: bool,
                 rel_similarity: bool,
                 rel_owns: bool,
                 rel_taxonomy: bool,
                 rel_version: bool,
                 rel_implies: bool):
        """
        :param token:
            the token to search on
        :param rel_partonomy:
            X partOf Y ; Y hasPart X
        :param rel_requires:
            X requires Y ; Y requiredBy X
        :param rel_similarity:
            X similarTo Y
            this is not the same as absolute equivalency but is helpful for understanding
            bidrectional relationship
        :param rel_owns:
            X owns Y ; Y ownedBy X
            'X' is typically an instance of company or organization
        :param rel_taxonomy:
            X rdf:type Y or X rdfs:subClassOf Y
            shows parent/child or instance/child relationships;
            a taxonomical view into the Ontology
        :param rel_version:
            X versionOf Y ; Y hasVersion X
            typically used in software (Windows hasVersion Windows XP)
        :param rel_implies:
            X implies Y
            this is a high-recall relationship and may result in many false-positive matches
            depending on the type of feedback required
        :return:
            a dictionary of specified relationships
        """

        d_inference = {}
        if rel_taxonomy:
            d_inference["children"] = self.parent_of(token)
            d_inference["parents"] = self.parents(token)
        if rel_version:
            d_inference["has_version"] = self.has_version(token)
            d_inference["versionOf"] = self.version_of(token)
        if rel_partonomy:
            d_inference["part_of"] = self.has_part(token)
            d_inference["has_part"] = self.part_of(token)
        if rel_requires:
            d_inference["requires"] = self.requires(token)
            d_inference["required_by"] = self.required_by(token)
        if rel_similarity:
            d_inference["similar"] = self.similar_to(token)
        if rel_implies:
            d_inference["implies"] = self.implies(token)
            d_inference["implied_by"] = self.implied_by(token)
        if rel_partonomy:
            d_inference["has_part"] = self.has_part(token)
            d_inference["part_of"] = self.part_of(token)
        if rel_owns:
            d_inference["owns"] = self.owns(token)
            d_inference["owned_by"] = self.owned_by(token)

        _d_inference = {}
        for rel_type in d_inference:
            if len(d_inference[rel_type]):
                _d_inference[rel_type] = sorted(set(d_inference[rel_type]))

        return {
            "token": token,
            "rels": _d_inference}

    """ ****************************************
                    INFINITIVE
    **************************************** """

    def infinitive_or_self(self,
                           some_token: str) -> str:
        """
        :param some_token:
        :return:
            either the infinitive form of the token or the token itself
        """
        results = self._forward_relationship(some_token,
                                             self.d_infinitive)
        if results and len(results):
            return results[0]
        return some_token

    """ ****************************************
                    SEE ALSO
    **************************************** """

    def see_also(self,
                 some_token: str,
                 bidirectional: bool = False) -> list:
        """
        :param some_token:
        :param bidirectional:
            True        use a bidirectional search
            False       one way resolution
            Reference   https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1442#issuecomment-16221710
        :return:
            a list of see-also with cardinality 0..*
        """
        results = set()

        if bidirectional:
            [results.add(x) for x in self._forward_relationship(some_token,
                                                                self.d_see_also)]
        [results.add(x) for x in self._reverse_relationship(some_token,
                                                            self.d_see_also)]

        results = [x for x in results if x != some_token]
        return sorted(results)

    """ ****************************************
                    IMPLIES
    **************************************** """

    def implies(self,
                some_token: str,
                common_implications: bool = False) -> list:
        """

        :param some_token:
            a token to search on
        :param common_implications:

            Given:
                'Python' implies 'Cognitive Skill'
                'Data Scientist' implies 'Cognitive Skill'
            Assume:
                search token='Python'
            Output:
                'Python' implies 'Cognitive Skill'

            What else implies 'Cognitive Skill'?
                'Python' --> 'Data Scientist'
            because both imply 'Cognitive Skill

            Reference
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/455#issuecomment-12796283
        :return:
        """
        results = self._forward_relationship(some_token,
                                             self.d_implies)
        if not common_implications:
            return results

        s = set(results)
        for result in results:
            [s.add(x) for x in self.implied_by(result) if x != some_token]

        return sorted(s)

    def implied_by(self,
                   some_token: str) -> list:
        return self._reverse_relationship(some_token,
                                          self.d_implies)

    def all_implies(self,
                    bidirectional: bool = False) -> dict:
        return self._all_relationships(self.d_implies,
                                       bidirectional=bidirectional)

    """ ****************************************
                    REQUIRES
    **************************************** """

    def requires(self,
                 some_token: str) -> list:
        return self._forward_relationship(some_token,
                                          self.d_requires)

    def required_by(self,
                    some_token: str) -> list:
        return self._reverse_relationship(some_token,
                                          self.d_requires)

    def all_requires(self,
                     bidirectional: bool = False) -> dict:
        return self._all_relationships(self.d_requires,
                                       bidirectional=bidirectional)

    """ ****************************************
                    RUNS ON
    **************************************** """

    def runsOn(self,
               some_token: str) -> list:
        return self._forward_relationship(some_token,
                                          self.d_runson)

    def all_runsOn(self,
                   bidirectional: bool = False) -> dict:
        return self._all_relationships(self.d_runson,
                                       bidirectional=bidirectional)

    """ ****************************************
                    PRODUCES
    **************************************** """

    def produces(self,
                 some_token: str) -> list:
        return self._forward_relationship(some_token,
                                          self.d_produces)

    def producedBy(self,
                   some_token: str) -> list:
        return self._reverse_relationship(some_token,
                                          self.d_produces)

    def all_produces(self,
                     bidirectional: bool = False) -> dict:
        return self._all_relationships(self.d_produces,
                                       bidirectional=bidirectional)

    """ ****************************************
                    VERSIONS
    **************************************** """

    def has_version(self,
                    some_token: str) -> list:
        return self._forward_relationship(some_token,
                                          self.d_versions)

    def version_of(self,
                   some_token: str) -> list:
        return self._reverse_relationship(some_token,
                                          self.d_versions)

    def all_versions(self,
                     bidirectional: bool = False) -> dict:
        return self._all_relationships(self.d_versions,
                                       bidirectional=bidirectional)

    """ ****************************************
                    PARENT/CHILD
    **************************************** """

    def has_all_ancestors(self,
                          some_token: str,
                          some_ancestors: list) -> bool:
        """
        :param some_token:
            any token             (e.g., 'Windows')
        :param some_ancestors:
            a list of candidate ancestors
        :return:
            True                if token has all of the ancestors
        """
        for ancestor in some_ancestors:
            if not self.has_ancestor(some_token, ancestor):
                return False
        return True

    def has_any_ancestors(self,
                          some_token: str,
                          some_ancestors: list) -> bool:
        """
        :param some_token:
            any token             (e.g., 'Windows')
        :param some_ancestors:
            a list of candidate ancestors
        :return:
            True                if token has at least one of the ancestors
        """
        for ancestor in some_ancestors:
            if self.has_ancestor(some_token, ancestor):
                return True
        return False

    def has_ancestor(self,
                     some_token: str,
                     some_ancestor: str) -> bool:
        """
        :param some_token:
            any token             (e.g., 'Windows')
        :param some_ancestor:
            a candidate ancestor  (e.g., 'Software')
        :return:
            True                if token and parent relationship exists
        """

        def _iter(a_value: str) -> bool:
            for a_parent in self.parents(a_value):
                if a_parent.lower().strip() == some_ancestor.lower().strip():
                    return True
                return _iter(a_parent)

        result = _iter(some_token)
        if not result or result is None:
            return False

        return result

    def has_parent(self,
                   some_token: str,
                   some_parent: str) -> bool:
        """
        :param some_token:
            any token           (e.g., 'Windows')
        :param some_parent:
            a candidate parent  (e.g., 'Operating System')
        :return:
            True                if token and parent relationship exists
        """
        return some_parent in self.parents(some_token)

    def parents(self,
                some_token: str) -> list:
        """
        :param some_token:
            any token
        :return:
            a list of all parents for this token
        """
        some_token = some_token.replace('_', ' ')  # GIT-1367-16010583
        return self._forward_relationship(some_token,
                                          self.d_parents)

    def ancestors(self,
                  some_token: str) -> list:
        """
        Purpose:
            Return entire ancestry tree up to the root node
        Example 1:
            Input:      'W3C standard'
            Output:     [[ 'standard', 'entity' ]]
        Example 2:
            Input:      'deep learning'
            Output:     [['cognitive skill', 'technical skill', 'skill'],
                         ['statistical algorithm', 'technical skill', 'skill']]
        :param some_token:
            any input token
        :return:
            a list of parents in ascending order
        """
        parents = []

        def inner_ancestors(a_token: str) -> None:
            for parent in self.parents(some_token=a_token):
                parents.append(parent)
                inner_ancestors(parent)

        inner_ancestors(some_token)

        # Step: Transform a single list
        #   ['cognitive skill', 'technical skill', 'skill', 'root', 'statistical algorithm', 'technical skill', 'skill', 'root']
        # into a a multi-list:
        #   [['cognitive skill', 'technical skill', 'skill'], ['statistical algorithm', 'technical skill', 'skill']]
        sequences = []
        curr_sequence = []
        for result in parents:
            if result != "root":
                curr_sequence.append(result)
            else:
                sequences.append(curr_sequence)
                curr_sequence = []

        return sequences

    def all_ancestors(self) -> list:
        """
        Purpose:
            An 'ancestor entity' is formally
                "<ancestor> rdfs:subClassOf owl:Thing"
        :return:
            a list of all 'ancestor' entities in the Cendant Ontology
        """
        ancestors = set()
        for k in self.d_parents:
            if "root" in self.d_parents[k]:
                ancestors.add(k)

        return sorted(ancestors)

    def parent_of(self,
                  some_token: str) -> list:
        """
        :param some_token:
            any token           (e.g., 'Operating System')
        :return:
            all the 'child' entities that this token is a parent of
                                (e.g., 'Windows, 'RedHat Linux', 'Ubuntu')
        """
        return self._reverse_relationship(some_token,
                                          self.d_parents)

    def all_parents(self,
                    bidirectional: bool = False) -> dict:
        """
        :param bidirectional:
        :return:
            the entire dictionary of parent relationships
        """
        return self._all_relationships(self.d_parents,
                                       bidirectional=bidirectional)

    def children(self,
                 some_token: str) -> list:
        """
        Purpose:
            A child is formally defined as any entity that is the subject of either a
                rdf:type
            or
                rdfs:subClassOf
            triple
        :param some_token:
            any Cendant entity
        :return:
            a list of all the direct children of an input entity
        """
        children = set()
        for k in self.d_parents:
            for v in self.d_parents[k]:
                if v == some_token:
                    children.add(k)

        return sorted(children)

    """ ****************************************
                    PARTONOMY
    **************************************** """

    def has_part(self,
                 some_token: str) -> list:
        return self._forward_relationship(some_token,
                                          self.d_parts)

    def part_of(self,
                some_token: str) -> list:
        return self._reverse_relationship(some_token,
                                          self.d_parts)

    def all_parts(self,
                  bidirectional: bool = False) -> dict:
        return self._all_relationships(self.d_parts,
                                       bidirectional=bidirectional)

    """ ****************************************
                    OWNERSHIP
    **************************************** """

    def owns(self,
             some_token: str,
             include_children: bool = False) -> list:
        """
        :param some_token:
            any token (typically a company or corporate entity)
                                    (e.g., 'Microsoft')
        :param include_children:
            include child nodes in the ownership inference

            Example:
                'RedHat Linux' owns 'OpenShift'
                'OpenShift Origin' rdf:type 'OpenShift'
                'OpenShift Online' rdf:type 'OpenShift'
                'OpenShift Dedicated' rdf:type 'OpenShift'
            Returns:
                [   'OpenShift',
                    'OpenShift Origin',
                    'OpenShift Online',
                    'OpenShift Dedicated' ]
            if this parameter was set to False, only
                [   'OpenShift' ]
            would be returned

        :return:
            a list of entities owned by this token
                                    (e.g., ['Windows', 'MS Office'])
        """

        results = self._forward_relationship(some_token,
                                             self.d_owns)
        if not include_children:
            return results

        s = set(results)
        for result in results:
            [s.add(x) for x in self.parent_of(result)]
        return sorted(s)

    def is_owned_by(self,
                    some_token: str,
                    some_owner: str) -> bool:
        """
        :param some_token:
            any token               (e.g., 'Windows')
        :param some_owner:
            a candidate owner       (e.g., 'Microsoft')
        :return:
            True                    if the ownership relationship exists
        """
        return some_owner in self.owned_by(some_token)

    def owned_by(self,
                 some_token: str) -> list:
        """
        :param some_token:
            any token               (e.g., 'Windows')
        :return:
            a list of owners        (e.g., ['Microsoft'])
        """
        return self._reverse_relationship(some_token,
                                          self.d_owns)

    def all_owns(self,
                 bidirectional: bool = False) -> dict:
        """
        :param bidirectional:
        :return:
            the entire ownership dictionary
        """
        return self._all_relationships(self.d_owns,
                                       bidirectional=bidirectional)

    """ ****************************************
                    DEFINES
    **************************************** """

    def defines(self,
                some_token: str) -> list:
        return self._forward_relationship(some_token,
                                          self.d_defines)

    def defined_by(self,
                   some_token: str) -> list:
        return self._reverse_relationship(some_token,
                                          self.d_defines)

    def all_defines(self,
                    bidirectional: bool = False) -> dict:
        return self._all_relationships(self.d_defines,
                                       bidirectional=bidirectional)

    """ ****************************************
                    REFERENCES
    **************************************** """

    def all_references(self,
                       some_token: str,
                       references_minimum_frequency: int = 2.0,
                       references_maximum_zscore: float = 1.5,
                       referenced_by_minimum_frequency: int = 5.0,
                       referenced_by_maximum_zscore: float = 1.5) -> list:
        s = set()
        [s.add(x) for x in self.references(some_token,
                                           minimum_frequency=references_minimum_frequency,
                                           maximum_zscore=references_maximum_zscore)]
        [s.add(x) for x in self.referenced_by(some_token,
                                              minimum_frequency=referenced_by_minimum_frequency,
                                              maximum_zscore=referenced_by_maximum_zscore)]

        return sorted(s)

    def references_transitive(self,
                              a_tag: str) -> list:

        freq = 2
        zscr = 1.5
        s_unique = set()

        def _inner_references(some_tag: str,
                              minimum_frequency: int,
                              maximum_zscore: float) -> list:
            s_unique.add(some_tag)
            return self.references(some_tag,
                                   minimum_frequency=minimum_frequency,
                                   maximum_zscore=maximum_zscore)

        for inner_tag in _inner_references(a_tag, freq, zscr):
            if inner_tag not in s_unique:
                _inner_references(inner_tag, freq + 1, zscr - 0.2)

        return sorted([x for x in s_unique if x != a_tag])

    def references(self,
                   some_token: str,
                   minimum_frequency: int = 2,
                   maximum_zscore: float = 2.0) -> list:

        some_token = self._cleanse(some_token)
        for key in self.d_references:
            if self._cleanse(key) == some_token:

                def _valid(some_tag: str) -> bool:
                    v = self.d_references[key][some_tag]
                    if v["z"] > maximum_zscore:
                        return False
                    if v["f"] < minimum_frequency:
                        return False
                    return True

                references = [x for x in self.d_references[key]
                              if _valid(x)]
                return [self.entity_finder.label_or_self(x) for x in references
                        if self._cleanse(x) != some_token]

        return []

    def referenced_by(self,
                      some_token: str,
                      minimum_frequency: int = 5,
                      maximum_zscore: float = 2.0) -> list:
        s = set()
        some_token = self._cleanse(some_token)
        for key in self.d_references:
            for value in self.d_references[key]:
                if self._cleanse(value) == some_token:

                    def _valid(some_tag: str) -> bool:
                        v = self.d_references[key][some_tag]
                        if v["zscore"] > maximum_zscore:
                            return False
                        if v["frequency"] < minimum_frequency:
                            return False
                        return True

                    [s.add(x) for x in self.d_references[key]
                     if _valid(x)]

        s = sorted([self.entity_finder.label_or_self(x) for x in s
                    if self._cleanse(x) != some_token])

        return sorted(s)

    """ ****************************************
                    SIMILARITY
    **************************************** """

    def similar_to(self,
                   some_token: str) -> list:
        r1 = self._forward_relationship(some_token,
                                        self.d_similarity)
        r2 = self._reverse_relationship(some_token,
                                        self.d_similarity)
        return sorted(set(r1 + r2))

    def all_similar(self,
                    bidirectional: bool = False) -> dict:
        return self._all_relationships(self.d_similarity,
                                       bidirectional=bidirectional)


if __name__ == "__main__":
    print(FindRelationships().has_ancestor("Biophysics", "Skill"))
