# !/usr/bin/env python
# -*- coding: UTF-8 -*-


from base import BaseObject


class DictionaryLoader(BaseObject):
    """ Load dictionaries by Ontology """

    _base_names = ['base', 'cendant']

    _biotech_names = ['biotech']

    def __init__(self,
                 ontology_name: str,
                 is_debug: bool = False):
        """
        Created:
            13-Dec-2019
            craig.trim@ibm.com
            *   https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1582#issuecomment-16611092
        Updated:
            13-Feb-2020
            craig.trim@ibm.com
            *   add ontology names in list
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1853https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1853
        """
        BaseObject.__init__(self, __name__)

        self._is_debug = is_debug
        self._ontology_name = ontology_name.lower().strip()

    def _error(self):
        self.logger.error(f"Ontology Name Not Recognized: "
                          f"{self._ontology_name}")
        raise NotImplementedError

    def synonyms(self):
        class Facade(object):

            @classmethod
            def fwd(cls):
                if self._ontology_name in self._base_names:
                    from datadict.core.os.base import the_synonyms_dict
                    return the_synonyms_dict
                if self._ontology_name in self._biotech_names:
                    from datadict.core.os.biotech import the_synonyms_dict
                    return the_synonyms_dict
                self._error()

            @classmethod
            def rev(cls):
                if self._ontology_name in self._base_names:
                    from datadict.core.os.base import the_reverse_synonym_dict
                    return the_reverse_synonym_dict
                if self._ontology_name in self._biotech_names:
                    from datadict.core.os.biotech import the_reverse_synonym_dict
                    return the_reverse_synonym_dict
                self._error()

            @classmethod
            def seeAlso(cls):
                if self._ontology_name in self._base_names:
                    from datadict.core.os.base import the_seealso_dict
                    return the_seealso_dict
                if self._ontology_name in self._biotech_names:
                    from datadict.core.os.biotech import the_seealso_dict
                    return the_seealso_dict
                self._error()

        return Facade()

    def relationships(self):
        class Facade(object):

            @classmethod
            def similarity(cls):
                if self._ontology_name in self._base_names:
                    from datadict.core.os.base import the_rel_similarity_dict
                    return the_rel_similarity_dict
                if self._ontology_name in self._biotech_names:
                    from datadict.core.os.biotech import the_rel_similarity_dict
                    return the_rel_similarity_dict
                self._error()

            @classmethod
            def requires(cls):
                if self._ontology_name in self._base_names:
                    from datadict.core.os.base import the_rel_requires_dict
                    return the_rel_requires_dict
                if self._ontology_name in self._biotech_names:
                    from datadict.core.os.biotech import the_rel_requires_dict
                    return the_rel_requires_dict
                self._error()

            @classmethod
            def parts(cls):
                if self._ontology_name in self._base_names:
                    from datadict.core.os.base import the_rel_parts_dict
                    return the_rel_parts_dict
                if self._ontology_name in self._biotech_names:
                    from datadict.core.os.biotech import the_rel_parts_dict
                    return the_rel_parts_dict
                self._error()

            @classmethod
            def defines(cls):
                if self._ontology_name in self._base_names:
                    from datadict.core.os.base import the_rel_defines_dict
                    return the_rel_defines_dict
                if self._ontology_name in self._biotech_names:
                    from datadict.core.os.biotech import the_rel_defines_dict
                    return the_rel_defines_dict
                self._error()

            @classmethod
            def versions(cls):
                if self._ontology_name in self._base_names:
                    from datadict.core.os.base import the_rel_versions_dict
                    return the_rel_versions_dict
                if self._ontology_name in self._biotech_names:
                    from datadict.core.os.biotech import the_rel_versions_dict
                    return the_rel_versions_dict
                self._error()

            @classmethod
            def runsOn(cls):
                if self._ontology_name in self._base_names:
                    from datadict.core.os.base import the_rel_runson_dict
                    return the_rel_runson_dict
                if self._ontology_name in self._biotech_names:
                    from datadict.core.os.biotech import the_rel_runson_dict
                    return the_rel_runson_dict
                self._error()

            @classmethod
            def produces(cls):
                if self._ontology_name in self._base_names:
                    from datadict.core.os.base import the_rel_produces_dict
                    return the_rel_produces_dict
                if self._ontology_name in self._biotech_names:
                    from datadict.core.os.biotech import the_rel_produces_dict
                    return the_rel_produces_dict
                self._error()

            @classmethod
            def owns(cls):
                if self._ontology_name in self._base_names:
                    from datadict.core.os.base import the_rel_owns_dict
                    return the_rel_owns_dict
                if self._ontology_name in self._biotech_names:
                    from datadict.core.os.biotech import the_rel_owns_dict
                    return the_rel_owns_dict
                self._error()

            @classmethod
            def implies(cls):
                if self._ontology_name in self._base_names:
                    from datadict.core.os.base import the_rel_implies_dict
                    return the_rel_implies_dict
                if self._ontology_name in self._biotech_names:
                    from datadict.core.os.biotech import the_rel_implies_dict
                    return the_rel_implies_dict
                self._error()

            @classmethod
            def infinitive(cls):
                if self._ontology_name in self._base_names:
                    from datadict.core.os.base import the_rel_infinitive_dict
                    return the_rel_infinitive_dict
                if self._ontology_name in self._biotech_names:
                    from datadict.core.os.biotech import the_rel_infinitive_dict
                    return the_rel_infinitive_dict
                self._error()

        return Facade()

    def taxonomy(self):
        class Facade(object):

            @classmethod
            def labels(cls):
                if self._ontology_name in self._base_names:
                    from datadict.core.os.base import the_labels_dict
                    return the_labels_dict
                if self._ontology_name in self._biotech_names:
                    from datadict.core.os.biotech import the_labels_dict
                    return the_labels_dict
                self._error()

            @classmethod
            def parents(cls):
                if self._ontology_name in self._base_names:
                    from datadict.core.os.base import the_parents_dict
                    return the_parents_dict
                if self._ontology_name in self._biotech_names:
                    from datadict.core.os.biotech import the_parents_dict
                    return the_parents_dict
                self._error()

            @classmethod
            def patterns(cls):
                if self._ontology_name in self._base_names:
                    from datadict.core.os.base import the_patterns_dict
                    return the_patterns_dict
                if self._ontology_name in self._biotech_names:
                    from datadict.core.os.biotech import the_patterns_dict
                    return the_patterns_dict
                self._error()

            @classmethod
            def ngrams(cls):
                if self._ontology_name in self._base_names:
                    from datadict.core.os.base import the_entity_ngrams
                    return the_entity_ngrams
                if self._ontology_name in self._biotech_names:
                    from datadict.core.os.biotech import the_entity_ngrams
                    return the_entity_ngrams
                self._error()

        return Facade()
