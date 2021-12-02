#!/usr/bin/env python
# -*- coding: UTF-8 -*-


import pprint

from base import BaseObject
from base import MandatoryParamError


class GraphFieldMapping(BaseObject):
    """ """

    def __init__(self,
                 some_manifest_fields: list,
                 some_source_records: list):
        """
        Created:
            15-Mar-2019
            craig.trim@ibm.com
            *   refactored out of 'load-neo-from-manifest-2'
        :param some_manifest_fields:
            the field definitions in the manifest
        :param some_source_records:
            the source records from mongoDB
        """
        BaseObject.__init__(self, __name__)
        if not some_manifest_fields:
            raise MandatoryParamError("Manifest Fields")
        if not some_source_records:
            raise MandatoryParamError("Source Records")

        self.manifest_fields = some_manifest_fields
        self.source_records = some_source_records

    def _transform_source_record(self,
                                 source_record: dict) -> dict:
        """
            map a single source record from mongo with a corresponding manifest record

            sample input:

                Manifest:
                    - source_name: job_role
                      target_name: JobRole
                    - source_name: job_role_id
                      target_name: JobRoleId
                      relationships:
                        - partOf:
                            - job_role
                    - source_name: skill_set
                      target_name: SkillSet

                Source Record:
                    [   {   'name': 'job_role',
                            'type': 'text',
                            'value': 'Application Database Administrator' },
                        {   'name': 'job_role_id',
                            'type': 'text',
                            'value': '042523' },
                        {   'name': 'skill_set',
                            'type': 'text',
                            'value': 'Application Database Administrator' }]

            sample output:
                [   {   'functional_community': {
                            'field': [
                                {  'name': 'functional_community',
                                    'type': 'text',
                                    'value': 'GTS Delivery' } ],
                            'manifest': {
                                'source_name': 'functional_community',
                                'target_name': 'FunctionalCommunity' }},

                        'job_category': {
                            'field': [
                                {   'name': 'job_category',
                                    'type': 'text',
                                    'value': 'Technical Specialist' } ],
                            'manifest': {
                                'source_name': 'job_category',
                                'target_name': 'JobCategory' }},

                        ...

                        'job_role': {
                            'field': [
                                {   'name': 'job_role',
                                    'type': 'text',
                                    'value': 'Application Database Administrator' } ],
                            'manifest':
                                {   'source_name': 'job_role',
                                    'target_name': 'JobRole' }},
                    }]
        :param source_record:
            a single record (JSON dictionary) from MongoDB
        :return:
            a dictionary of mongoDB records mapped to Manifest fields
        """

        d_record = {}
        for manifest_field in self.manifest_fields:




            source_field = [source_field for source_field in source_record["fields"] if
                            source_field["name"] == manifest_field["source_name"]]

            if not len(source_field):
                self.logger.warn("\n".join([
                    "Source Field Not Found",
                    pprint.pformat(source_field)
                ]))

                continue

            if not source_field[0]["value"]:
                continue

            # hack start
            text = source_field[0]["value"]
            if ".0" in text:
                text = text.split(".0")[0].strip()
            source_field[0]["value"] = text
            # hack end

            d_record[manifest_field["source_name"]] = {
                "field": source_field,
                "manifest": manifest_field
            }

        return d_record

    def process(self) -> list:
        """ map mongo fields to manifest fields
            merges each source record from mongo with a manifest record
        """

        l_records = [self._transform_source_record(x) for x in self.source_records]

        self.logger.debug("\n".join([
            "Mapped Source Records to Manifest Fields",
            "\ttotal-records: {}".format(len(l_records))
        ]))

        return l_records
