#!/usr/bin/env python
# -*- coding: UTF-8 -*-


from py2neo import Relationship
from py2neo import Transaction

from base import BaseObject
from base import MandatoryParamError
from datagraph import NeoGraphConnector
from datagraph import NeoGraphContext
from nlutext import TextParser


class LoadNeoFromManifest(BaseObject):
    """ """

    def __init__(self,
                 some_manifest_name: str,
                 some_activity_name: str):
        """
        Created:
            14-Mar-2019
            craig.trim@ibm.com
        Updated:
            26-Mar-2019
            craig.trim@ibm.com
            *   updates based on MDA changes
        :param some_manifest_name:
            the name of the manifest
        :param some_activity_name:
            the name of the activity within the manifest
        """
        BaseObject.__init__(self, __name__)
        if not some_manifest_name:
            raise MandatoryParamError("Manifest Name")
        if not some_activity_name:
            raise MandatoryParamError("Activity Name")

        self.manifest_name = some_manifest_name
        self.activity_name = some_activity_name

        self.text_parser = TextParser()
        self.graph_context = NeoGraphContext()

    @staticmethod
    def _mongo_sources(d_manifest: dict) -> list:
        """ return all the defined mongo sources"""

        def _is_mongo(a_source):
            return a_source["source"]["description"]["type"].lower().strip() == "mongo"

        return [source["source"] for source
                in d_manifest["sources"] if _is_mongo(source)]

    @staticmethod
    def _mongo_collections(mongo_sources: list) -> dict:
        """ instantiate all the mongoDB connections """
        from dataingest.core.dmo import ManifestConnectorForMongo

        d_coll = {}
        for mongo_source in mongo_sources:
            conn = ManifestConnectorForMongo(mongo_source["description"]).process()
            d_coll[mongo_source["description"]["collection"]] = conn

        return d_coll

    def _generate_nodes(self,
                        tx: Transaction,
                        mapped_source_records: list,
                        mongo_source: dict):
        """ generate all the nodes"""

        node_entity = "{} - {}".format(mongo_source["description"]["database"],
                                       mongo_source["description"]["collection"])

        for source_record in mapped_source_records:
            for field in source_record:
                sentence = " ".join(field["value"])

                field_node = self.graph_context.find_or_create_node(tx,
                                                                    sentence,
                                                                    field["name"],
                                                                    some_entity=node_entity)

                for tag in field["tags"]:
                    tag_node = self.graph_context.find_or_create_node(tx,
                                                                      tag["name"],
                                                                      "DomainTerm",
                                                                      some_activity=tag["type"],
                                                                      some_entity=tag["provenance"])
                    tx.create(Relationship(field_node,
                                           "implies-type1",
                                           tag_node,
                                           activity="Known Relationship",
                                           entity=""))

                # add subsumes implications
                for tag in field["tags"]:
                    _s = self.graph_context.find_or_create_node(tx,
                                                                tag["name"],
                                                                "DomainTerm",
                                                                some_activity=tag["type"],
                                                                some_entity=tag["provenance"])

                    for tag_link in tag["links"]:
                        _o = self.graph_context.find_or_create_node(tx,
                                                                    tag_link,
                                                                    "DomainTerm",
                                                                    some_activity=tag["type"],
                                                                    some_entity=tag["provenance"])

                        tx.create(Relationship(_s,
                                               "implies-type2",
                                               _o,
                                               activity="Inferred Relationship",
                                               entity="Vector Space"))

    def _generate_relationships(self,
                                tx: Transaction,
                                mapped_source_records: list):
        """ generate all the relationships """

        for source_record in mapped_source_records:

            for field in source_record:
                _subject = self.graph_context.find_or_create_node(tx,
                                                                  " ".join(field["value"]),
                                                                  field["name"], )

                for rel in field["relationships"]:
                    _object = self.graph_context.find_or_create_node(tx,
                                                                     " ".join(rel["value"]),
                                                                     rel["name"])
                    tx.create(Relationship(_subject,
                                           rel["type"],
                                           _object,
                                           activity="Known Relationship"))

    def process(self):
        from dataingest.core.dmo import ManifestActivityFinder
        from dataingest.graph.dmo import GraphDataExtractor
        from dataingest.graph.dmo import GraphFieldMapping
        from dataingest.graph.dmo import GraphDataTransform
        from datagraph import GenerateSimilarityMetric

        d_manifest = ManifestActivityFinder(self.manifest_name,
                                            self.activity_name).process()

        mongo_sources = self._mongo_sources(d_manifest)
        mongo_collections = self._mongo_collections(mongo_sources)

        neog = NeoGraphConnector().connect(connection_type="local")
        neog.delete_all()

        tx = neog.begin()

        for mongo_source in mongo_sources:
            source_records = GraphDataExtractor(mongo_collections,
                                                mongo_source).process()

            # retrieve fields by manifest
            manifest_fields = [manifest_field for manifest_field in mongo_source["fields"]]
            mapped_source_records = GraphFieldMapping(manifest_fields, source_records).process()
            mapped_source_records = GraphDataTransform(mapped_source_records,
                                                       tag_min_threshold=2).process()

            # create all the nodes
            self._generate_nodes(tx, mapped_source_records, mongo_source)
            self._generate_relationships(tx, mapped_source_records)

        GenerateSimilarityMetric(tx, self.graph_context).process()
        tx.commit()

        self.logger.info("\n".join([
            "Neo Graph Ingest Completed",
            "\ttotal-nodes: {}".format(self.graph_context.total_nodes())
        ]))
