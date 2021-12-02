#!/usr/bin/env python
# -*- coding: UTF-8 -*-


from base import BaseObject
from base import LabelFormatter
from base import MandatoryParamError


class GraphDataTransform(BaseObject):
    """ """

    def __init__(self,
                 some_mapped_source_records: list,
                 tag_min_threshold=1):
        """
        Created:
            15-Mar-2019
            craig.trim@ibm.com
            *   refactored out of 'load-neo-from-manifest-2'
        Updated:
            5-Apr-2019
            craig.trim@ibm.com
            *   added 'properties' attribute
        :param some_mapped_source_records:
            the name of the manifest
        """
        BaseObject.__init__(self, __name__)
        if not some_mapped_source_records:
            raise MandatoryParamError("Source Records")

        self.node_lookup = {}
        self.root_lookup = {}
        self.template_cache = {}

        self.tag_min_threshold = tag_min_threshold
        self.mapped_source_records = some_mapped_source_records
        self.label_formatter = LabelFormatter()

    @staticmethod
    def _map_relationships(source_record: dict,
                           field_name: str) -> list:
        """ map relationships

            the manifest may specify a relationship like this:
                - source_name: job_role_id
                  target_name: JobRoleId
                  relationships:
                    - partOf:
                        - job_role

            use the output defined in 'map-source-records' and
            for each specified relationship return a list of dictionaries

            where each dictionary looks like:

                {   'type':     'partOf',
                    'name':     'job_role',
                    'value':    'Application Developer'    }
        """
        mapped_relationships = []
        for d_relationship in source_record[field_name]["manifest"]["relationships"]:
            for relationship_type in d_relationship:
                for related_field_name in d_relationship[relationship_type]:

                    # presumably the source record was blank
                    if related_field_name not in source_record:
                        continue

                    mapped_relationships.append({
                        "type": relationship_type,
                        "name": source_record[related_field_name]["manifest"]["target_name"],
                        "value": source_record[related_field_name]["field"][0]["value"]
                    })

        return mapped_relationships

    @staticmethod
    def _map_properties(source_record: dict,
                        field_name: str) -> list:
        """ map properties

            the manifest may specify a property like this:
                - source_name: job_role_id
                  target_name: JobRoleId
                  properties:
                    - name: job_role_name

            use the output defined in 'map-source-records' and
            for each specified property return a list of dictionaries

            where each dictionary looks like:

                {   'type':     'property',
                    'name':     'name',
                    'value':    '<Some Job Role Name>'    }
        """
        mapped_properties = []
        for d_property in source_record[field_name]["manifest"]["properties"]:
            _key = list(d_property.keys())[0]
            _value = d_property[_key]
            mapped_properties.append({
                "name": _key,
                "value": source_record[_value]["field"][0]["value"]
            })

        return mapped_properties

    def _extract_tags_from_source_field(self,
                                        a_field: dict,
                                        provenance="Cendant Ontology") -> list:

        def _tags(some_tags: list) -> list:
            _tag_set = set(some_tags)
            _tag_set = [x for x in _tag_set if x]
            _tag_set = [self.label_formatter.process(x) for x in _tag_set]

            return sorted(_tag_set)

        if "tags" not in a_field:
            return []

        def _links(a_tag: str,
                   all_tags: list) -> list:
            s = set()
            for _tag in all_tags:
                if _tag == a_tag:
                    continue
                if _tag in a_tag:
                    s.add(_tag)
            return sorted(s)

        l_tags = []

        def _generate(all_tags: list,
                      some_type: str):
            for a_tag in all_tags:
                l_tags.append({
                    "name": a_tag,
                    "type": some_type,
                    "provenance": provenance,
                    "links": _links(a_tag, all_tags)
                })

        supervised_tags = _tags(a_field["tags"]["supervised"])
        _generate(supervised_tags, "supervised-learning")

        unsupervised_tags = _tags(a_field["tags"]["unsupervised"])
        _generate(unsupervised_tags, "unsupervised-learning")

        return l_tags

    def process(self,
                threshold=100) -> list:
        """ add actual relationships to each mapped record """

        ctr = 0
        l_records = []
        total_records = len(self.mapped_source_records)

        for source_record in self.mapped_source_records:

            l_record = []
            for field in source_record:

                def _has_relationships() -> bool:
                    return "relationships" in source_record[field]["manifest"]

                def _has_properties() -> bool:
                    return "properties" in source_record[field]["manifest"]

                def _has_create_node() -> bool:
                    return "create_node" in source_record[field]["manifest"]

                def _relationships() -> list:
                    """
                    :return:
                        relationships form edges (lines) on the graph
                        and create connectivity between ndoes
                    """
                    if _has_relationships():
                        return self._map_relationships(source_record, field)
                    return []

                def _properties() -> list:
                    """
                    :return:
                        properties are key/value pairs that are associated with nodes
                    """
                    if _has_properties():
                        return self._map_properties(source_record, field)
                    return []

                def _create_node() -> bool:
                    """
                    :return:
                        some nodes are only used informationally as properties for other nodes
                        and as such should not be generated onto a graph
                    """
                    if _has_create_node():
                        return source_record[field]["manifest"]["create_node"]
                    return True

                def _target_name() -> str:
                    """
                    :return:
                        the target name of the field
                        (may be the same as the source name)
                    """
                    if "target_name" in source_record[field]["manifest"]:
                        return source_record[field]["manifest"]["target_name"]
                    return source_record[field]["manifest"]["source_name"]

                first_field = source_record[field]["field"][0]
                _tags = self._extract_tags_from_source_field(first_field)

                l_record.append({
                    "name": _target_name(),
                    "tags": _tags,
                    "type": first_field["type"],
                    "value": first_field["value"],
                    "properties": _properties(),
                    "create_node": _create_node(),
                    "relationships": _relationships()})

            ctr += 1
            if ctr % threshold == 0:
                self.logger.debug("\n".join([
                    "Processing {} - {}".format(ctr, total_records)]))

            l_records.append(l_record)

        l_records = [x for x in l_records if x and len(x)]
        return l_records
