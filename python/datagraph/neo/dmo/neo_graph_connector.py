#!/usr/bin/env python
# -*- coding: UTF-8 -*-


import os

from py2neo.database import Graph

from base import FileIO


class NeoGraphConnector:
    """
    Created:
        25-Feb-2019
        craig.trim@ibm.com
    """

    @classmethod
    def _load_creds(cls, connection):
        path = os.path.join(os.environ["CODE_BASE"], "resources/config/config.yml")
        doc = FileIO.file_to_yaml(path)
        return doc["neo"][connection]

    @classmethod
    def _neo_graph(cls, neo_creds):
        return Graph(neo_creds["url"],
                     auth=(neo_creds["username"],
                           neo_creds["password"]))

    @classmethod
    def connect(cls, connection_type="remote") -> Graph:
        return cls._neo_graph(cls._load_creds(connection_type))
