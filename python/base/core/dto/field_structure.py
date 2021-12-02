#!/usr/bin/env python
# -*- coding: UTF-8 -*-


import random

from base.core.dmo import BaseObject
from base.core.dmo import DataTypeError
from base.core.dmo import MandatoryParamError


class FieldStructure(object):
    """
    Created:
        20-Aug-2019
        craig.trim@ibm.com
        *   refactored out of existing components
    Updated:
        26-Aug-2019
        craig.trim@ibm.com
        *   transform field-id from int to str
            I wasn't able to query MongoDB with int values
    """

    @classmethod
    def field_id(cls,
                 field_value: str) -> str:
        fid = hash(field_value) + random.randint(1, 21) * 5
        if fid < 0:
            fid *= -1
        return str(fid)

    @classmethod
    def generate_tag_field(cls,
                           agent_name: str,
                           field_type: str,
                           field_name: str,
                           field_value: list,
                           field_value_normalized: list,
                           collection: str,
                           transformations: list,
                           field_id: str,
                           supervised_tags: list = None,
                           unsupervised_tags: list = None) -> dict:
        """
        Purpose:
            Generate a Field Structure for mongoDB records
            -   this function is specifically for 'Tag' fields.
            -   a 'Tag' field holds data that has passed through the Cendant annotation model
        :param field_value_normalized:
            the normalized field value
            Note:   this is the NLU pre-processed output prior to the annotation model run
        :param supervised_tags:
            the annotation tags produced by the Cendant NLU pipeline
            Note:   these tags are generated from the Cendant Ontology
        :param unsupervised_tags:
            the annotation tags produced by the Cendant NLU pipeline
            Note:   these tags are generated independently of the Cendant Ontology
        :param agent_name:
            reference 'generate_src_field' docstring
        :param field_type:
            reference 'generate_src_field' docstring
        :param field_name:
            reference 'generate_src_field' docstring
        :param field_value:
            reference 'generate_src_field' docstring
        :param collection:
            reference 'generate_src_field' docstring
        :param transformations:
            reference 'generate_src_field' docstring
        :param field_id:
            reference 'generate_src_field' docstring
        :return:
        """
        if not supervised_tags:
            supervised_tags = []
        if not unsupervised_tags:
            unsupervised_tags = []

        if not field_id:
            raise MandatoryParamError("Field ID")
        if type(supervised_tags) != list:
            raise DataTypeError("Supervised Tags, list")
        if type(unsupervised_tags) != list:
            raise DataTypeError("Unsupervised Tags, list")
        if not transformations:
            transformations = []

        field = cls.generate_src_field(agent_name=agent_name,
                                       field_type=field_type,
                                       field_name=field_name,
                                       collection=collection,
                                       transformations=transformations,
                                       field_value=field_value,
                                       field_id=field_id)

        field['tags'] = {
            "supervised": supervised_tags,
            "unsupervised": unsupervised_tags}

        field['normalized'] = field_value_normalized

        return field

    @classmethod
    def generate_src_field(cls,
                           agent_name: str,
                           field_type: str,
                           field_name: str,
                           field_value: int or str or float or None,
                           transformations: list = None or None,
                           collection: str = None,
                           field_id: str = None) -> dict:
        """
        Purpose:
            Generate a Field Structure for mongoDB records
        Implementation Notes:
            To properly understand this structure, Cendant mongoDB collections must be understood.
            -   Cendant collections contain records; each record contains 0..* fields
            -   this is the field data structure and validator
        :param field_id:
            an artificial PK to identify a field in a record
            Note:   each mongoDB record (particularly in the source collections) can contain 100s of fields
                    for testing/debugging, it's helpful to precisely identify the specific field in question
        :param agent_name:
            the agent responsible for creating the data
            e.g.,   System or Agent
                    System  implies that the information is system-generated
                    User    implies that the information is user-supplied
        :param field_type:
            the field type
            Note:   Used to determine NLU and Rules processing
                    e.g.,   fields with type='long-text' are sent through the NLU pipeline
                            fields with type='badge' are sent through the Badge Rules pipeline
                            fields with type='text' are largely ignored
        :param field_name:
            the name of the field
            Note    this is typically the database column name that the source data came from
                    e.g.,   'badge' or 'profile' or 'position_start_year'
        :param field_value:
            the actual field value
            Note:   there is a broad variety of acceptable data types
                    in 'source' collections this a non-list value (such as str, int, float, etc)
                    in 'tag' collections this needs to be a list
                        in order to contain multiple values if the original value is post-processed
                        post-processing is not mandatory, but the list is needed for consistency
        :param collection:
            the collection the data came from
            Note:   this is only relevant in assembled fields;
                    not in ingested fields (though this could be altered)
        :param transformations:
            a list of transformations performed on the data
            Note:   transformations are typically defined in the ingest and assembly manifests
                    and correspond to pre-defined ETL activities
                    e.g., 'lookup-city' or 'cleanse-name'
        :return:
            a field data structure
        """

        if not field_id:
            field_id = BaseObject.generate_tts(ensure_random=True)

        if not agent_name:
            raise MandatoryParamError("Field Agent")

        if type(field_id) != str:
            raise DataTypeError("Field ID, str")

        if not transformations:
            transformations = []

        if type(transformations) != list:
            raise DataTypeError("Transformations, list")

        agent_name = agent_name.lower()

        field = {
            "field_id": field_id,
            "agent": agent_name,
            "type": field_type,
            "name": field_name,
            "value": field_value,
            "transformations": transformations}

        if collection:
            field["collection"] = collection

        return field
