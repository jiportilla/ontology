# !/usr/bin/env python
# -*- coding: UTF-8 -*-


import pprint
from random import randint

from base import BaseObject
from datagraph import DotGraphContext
from datagraph import DotGraphGenerator
from datagraph import DotNodeGenerator
from datagraph import DotRelationshipGenerator
from datagraph import NeoGraphConnector
from datagraph import NeoGraphContext
from datagraph import NeoNodeGenerator
from datagraph import NeoRelationshipGenerator
from dataingest import GraphDataTransform
from dataingest import GraphFieldMapping
from dataingest import ManifestActivityFinder
from dataingest import ManifestConnectorForMongo


class GraphAttendees(BaseObject):
    """
    """

    def __init__(self):
        """
        Created:
            4-Apr-2019
            craig.trim@ibm.com
        """
        BaseObject.__init__(self, __name__)

    def _source_records(self,
                        manifest_source: dict):
        """
        :param manifest_source:
        :return:
            source records
        """

        def _records():
            col = ManifestConnectorForMongo(manifest_source["description"]).process()

            if "limit" in manifest_source["description"]:
                limit = manifest_source["description"]["limit"]
                if limit:
                    try:
                        return col.random(total_records=int(limit))
                    except Exception:
                        self.logger.error("\n".join([
                            "Unrecognized 'Limit' Parameter",
                            manifest_source["description"]
                        ]))

            return col.all()

        source_records = _records()

        # Step: Map the Source Records
        # transforms source records into a format suitable for loading into a Neo Graph
        mapped_source_records = GraphFieldMapping(manifest_source["fields"],
                                                  source_records).process()
        mapped_source_records = GraphDataTransform(mapped_source_records,
                                                   tag_min_threshold=2).process()

        _rand = randint(0, len(mapped_source_records))
        self.logger.debug("\n".join([
            "Retrieved Source Records",
            "\ttotal: {}".format(len(source_records)),
            "\tRandom Record [{}]".format(_rand),
            pprint.pformat(mapped_source_records[_rand],
                           indent=4)
        ]))

        return mapped_source_records

    def process(self):
        # Step: Connect to Neo Graph
        neog = NeoGraphConnector.connect(connection_type="local")

        # Step: Being a Transaction and Clear Prior Data
        tx = neog.begin()
        neog.delete_all()

        # Step: Retrieve the Manifest
        # defines all sources of data and activities to be carried out
        d_manifest = ManifestActivityFinder(some_manifest_name="graph-budapest-conference",
                                            some_manifest_activity="Budapest Conference").process()

        # Step: Define the Context
        # this prevents nodes and edges from being duplicated
        neo_context = NeoGraphContext()
        dot_context = DotGraphContext()

        # Step: Retrieve the Source Recordscv_parse_employee_skills_profile
        for manifest_source in d_manifest["sources"]:
            manifest_source = manifest_source["source"]

            source_records = self._source_records(manifest_source)

            # TODO: Add OWL Relationships to the Mapped Source Records
            # should that be part of the manifest?  e.g. a new source (not MongoDB)
            # i dunno ... CendantOWL is the context behind every data source
            # the context enriches each data source as opposed to a datasource in its own right
            # OWL -> NEO sounds like an indepent service call (and one that likely already exists)

            # Step: Generate the Nodes
            neo_context = NeoNodeGenerator(some_tx=tx,
                                           context=neo_context,
                                           source_records=source_records,
                                           mongo_source=manifest_source).process()
            dot_context = DotNodeGenerator(context=dot_context,
                                           source_records=source_records,
                                           mongo_source=manifest_source).process()

            # Step: Generate the Relationships
            neo_context = NeoRelationshipGenerator(some_tx=tx,
                                                   context=neo_context,
                                                   source_records=source_records).process()
            dot_context = DotRelationshipGenerator(context=dot_context,
                                                   source_records=source_records).process()

        DotGraphGenerator(dot_context).process()
        # Step: Commit all changes to Neo
        tx.commit()


if __name__ == "__main__":
    GraphAttendees().process()
