#!/usr/bin/env python
# -*- coding: UTF-8 -*-


class EvidenceStructure(object):
    """
    Created:
        20-Aug-2019
        craig.trim@ibm.com
        *   refactored out of existing components
    """

    @classmethod
    def generate(cls,
                 key_field: str,
                 collection_name: str,
                 field_name: str,
                 normalized_text: str,
                 original_text: str,
                 tag_name: str,
                 tag_score: float,
                 tag_type: str,
                 schema_name: str) -> dict:
        """
        Purpose:
            Generates an Evidence Structure used in the Dimensionality algorithm
        Implementation Notes:
            -   A single 'record' in mongoDB has multiple 'fields'
            -   each field has 0..* evidence structure
            -   Thus, a single record has 0..* evidence structures
                (over 100 is common)
            -   these structures are aggregated into a DataFrame
            -   the DataFrame is used to compute dimensionality
        :param key_field:
            the primary identifier of the record
            e.g.,   'serial-number' or 'openseat-id' or 'learning-id'
        :param collection_name:
            the name of the source collection the field data came from
            e.g.,   'ingest_badges'
        :param field_name:
            the field name
            e.g.,   'badge_name'
        :param normalized_text:
            pre-processed text
            e.g.,   'mainframe_operator'
        :param original_text:
            original user text
            e.g.,   'MainFrame Operation'
        :param tag_name:
            the name (or value)
            e.g,    'Communications Server'
        :param tag_score:
            the tag confidence score
            e.g,    95.0
        :param tag_type:
            either 'Supervised' or 'Unsupervised'
            e.g.,   'Unsupervised'
        :param schema_name:
            the schema value associated with the tag-name
            e.g.,   'hard skill'
            Note:   the 'entity-schema-*' files can be found here:
                    https://github.ibm.com/-CDO/unstructured-analytics/tree/master/resources/config
        :return:
            an evidence dictionary
        """

        return {"Collection": collection_name,
                "KeyField": key_field,
                "FieldName": field_name,
                "NormalizedText": normalized_text,
                "OriginalText": original_text,
                "Tag": tag_name,
                "TagScore": tag_score,
                "TagType": tag_type,
                "Schema": schema_name}
