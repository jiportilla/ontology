#!/usr/bin/env python
# -*- coding: UTF-8 -*-


class KbNames(object):
    """
    Purpose:
        dictionary names
        the names of the dictionaries that are generated in the MDA.sh process
        names must correspond to __init__.py declarations
    """

    @staticmethod
    def antonyms():
        return "the_antonyms_kb"

    @staticmethod
    def rel_implies():
        return "the_rel_implies_dict"

    @staticmethod
    def rel_owns():
        return "the_rel_owns_dict"

    @staticmethod
    def rel_requires():
        return "the_rel_requires_dict"

    @staticmethod
    def rel_produces():
        return "the_rel_produces_dict"

    @staticmethod
    def rel_runson():
        return "the_rel_runson_dict"

    @staticmethod
    def rel_versions():
        return "the_rel_versions_dict"

    @staticmethod
    def flow_taxonomy():
        return "the_flow_taxonomy_dict"

    @staticmethod
    def flow_taxonomy_revmap():
        return "the_flow_taxonomy_revmap"

    @staticmethod
    def rel_infinitive():
        return "the_rel_infinitive_dict"

    @staticmethod
    def rel_similarity():
        return "the_rel_similarity_dict"

    @staticmethod
    def rel_parts():
        return "the_rel_parts_dict"

    @staticmethod
    def rel_defines():
        return "the_rel_defines_dict"

    @staticmethod
    def entity_ngrams():
        return "the_entity_ngrams"

    @staticmethod
    def stopwords():
        return "the_stopwords_dict"

    @staticmethod
    def synonyms():
        return "the_synonyms_dict"

    @staticmethod
    def seealso():
        return "the_seealso_dict"

    @staticmethod
    def references():
        return "the_references_dict"

    @staticmethod
    def dimesionality(a_name: str):
        return f"the_dimesionality_{a_name}_dict"

    @staticmethod
    def jrs_lookup():
        return "the_jrs_role_dict"

    @staticmethod
    def mapping():
        return "the_mapping_table_dict"

    @staticmethod
    def mapping_rev():
        return "the_mapping_rev_dict"

    @staticmethod
    def reverse_synonyms():
        return "the_reverse_synonym_dict"

    @staticmethod
    def reverse_regex_synonyms():
        return "the_reverse_regex_synonym_dict"

    @staticmethod
    def patterns():
        return "the_patterns_dict"

    @staticmethod
    def certifications():
        return "the_certifications_dict"

    @staticmethod
    def certs_hierarchy():
        return "the_certification_hierarchy_dict"

    @staticmethod
    def labels():
        return "the_labels_dict"

    @staticmethod
    def country_codes():
        return "the_country_code_dict"

    @staticmethod
    def city_to_region():
        return "the_city_to_region_dict"

    @staticmethod
    def country_to_region():
        return "the_country_to_region_dict"

    @staticmethod
    def parents():
        return "the_parents_dict"

    @staticmethod
    def templates():
        return "templates_dict"
