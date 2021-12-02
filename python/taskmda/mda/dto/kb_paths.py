#!/usr/bin/env python
# -*- coding: UTF-8 -*-


import logging

logger = logging.getLogger(__name__)

BASE_PATH = "python/datadict/core"


class KbPaths(object):
    """
    Purpose:
        paths for generated dictionaries
        these are output file paths used in MDA.sh process
        specifies which directories the generated files are placed in
    Updated:
        13-Dec-2019
        craig.trim@ibm.com
        *   add ontology-name as a param
            https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1583
    """

    @staticmethod
    def rel_implies(ontology_name: str):
        return f"{BASE_PATH}/os/{ontology_name}/rel_implies_kb.py"

    @staticmethod
    def rel_owns(ontology_name: str):
        return f"{BASE_PATH}/os/{ontology_name}/rel_owns_kb.py"

    @staticmethod
    def rel_requires(ontology_name: str):
        return f"{BASE_PATH}/os/{ontology_name}/rel_requires_kb.py"

    @staticmethod
    def rel_produces(ontology_name: str):
        return f"{BASE_PATH}/os/{ontology_name}/rel_produces_kb.py"

    @staticmethod
    def rel_runson(ontology_name: str):
        return f"{BASE_PATH}/os/{ontology_name}/rel_runson_kb.py"

    @staticmethod
    def rel_versions(ontology_name: str):
        return f"{BASE_PATH}/os/{ontology_name}/rel_versions_kb.py"

    @staticmethod
    def country_codes():
        return "{0}/os/{1}".format(BASE_PATH, "country_codes.py")

    @staticmethod
    def city_to_region():
        return "{0}/os/{1}".format(BASE_PATH, "city_region_kb.py")

    @staticmethod
    def country_to_region():
        return "{0}/os/{1}".format(BASE_PATH, "country_region_kb.py")

    @staticmethod
    def rel_infinitive(ontology_name: str):
        return f"{BASE_PATH}/os/{ontology_name}/rel_infinitive_kb.py"

    @staticmethod
    def rel_similarity(ontology_name: str):
        return f"{BASE_PATH}/os/{ontology_name}/rel_similarity_kb.py"

    @staticmethod
    def rel_parts(ontology_name: str):
        return f"{BASE_PATH}/os/{ontology_name}/rel_parts_kb.py"

    @staticmethod
    def rel_defines(ontology_name: str):
        return f"{BASE_PATH}/os/{ontology_name}/rel_defines_kb.py"

    @staticmethod
    def stopwords():
        return "{0}/os/{1}".format(BASE_PATH, "stopwords_kb.py")

    @staticmethod
    def synonyms(ontology_name: str):
        return f"{BASE_PATH}/os/{ontology_name}/synonym.py"

    @staticmethod
    def seealso(ontology_name: str):
        return f"{BASE_PATH}/os/{ontology_name}/seealso.py"

    @staticmethod
    def dimesionality(a_name: str):
        return "{0}/os/{1}".format(BASE_PATH, f"dimensionality_{a_name}_kb.py")

    @staticmethod
    def references():
        return f"{BASE_PATH}/os/seealso.py"

    @staticmethod
    def jrs_lookup():
        return "{0}/os/{1}".format(BASE_PATH, "jrs_lookup.py")

    @staticmethod
    def mapping():
        return "{0}/os/{1}".format(BASE_PATH, "mapping_table.py")

    @staticmethod
    def mapping_rev():
        return "{0}/os/{1}".format(BASE_PATH, "mapping_rev.py")

    @staticmethod
    def reverse_synonyms(ontology_name: str):
        return f"{BASE_PATH}/os/{ontology_name}/reverse_synonym.py"

    @staticmethod
    def reverse_regex_synonyms(ontology_name: str):
        return f"{BASE_PATH}/os/{ontology_name}/reverse_regex_synonym.py"

    @staticmethod
    def taxonomy():
        return "{0}/os/{1}".format(BASE_PATH, "taxonomy_kb.py")

    @staticmethod
    def taxonomy_revmap():
        return "{0}/os/{1}".format(BASE_PATH, "taxonomy_revmap_kb.py")

    @staticmethod
    def patterns(ontology_name:str):
        return f"{BASE_PATH}/os/{ontology_name}/patterns.py"

    @staticmethod
    def certifications():
        return "{0}/os/{1}".format(BASE_PATH, "certifications_kb.py")

    @staticmethod
    def certs_hierarchy():
        return "{0}/os/{1}".format(BASE_PATH, "certification_hierarchy_kb.py")

    @staticmethod
    def labels(ontology_name: str):
        return f"{BASE_PATH}/os/{ontology_name}/labels.py"

    @staticmethod
    def badges():
        return "{0}/os/{1}".format(BASE_PATH, "badges.py")

    @staticmethod
    def ngrams(ontology_name: str):
        return f"{BASE_PATH}/os/{ontology_name}/ngrams.py"

    @staticmethod
    def parents(ontology_name: str):
        return f"{BASE_PATH}/os/{ontology_name}/parents.py"

    @staticmethod
    def badge_analytics():
        return "{0}/os/{1}".format(BASE_PATH, "badge_analytics.py")

    @staticmethod
    def badge_distribution():
        return "{0}/os/{1}".format(BASE_PATH, "badge_distribution.py")
