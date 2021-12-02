#!/usr/bin/env python
# -*- coding: UTF-8 -*-


import json

import networkx as nx
from networkx.readwrite import json_graph

from base import BaseObject
from base import MandatoryParamError


class SentimentAnalysis(BaseObject):
    """ Create Sentiment Analysis """

    def __init__(self,
                 informal_graph: nx,
                 formal_graph: nx):
        """
        Created:
            - 30-July-2019
            - abhbasu3@in.ibm.com
        Updated:
            13-September-2019
            abhbasu3@in.ibm.com
            *   added betweenness centrality attribute to the sna graph
            *   https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/927
        :param informal_graph:
        :param formal_graph:
        """
        BaseObject.__init__(self, __name__)

        if not informal_graph:
            raise MandatoryParamError("Informal Graph")
        if not formal_graph:
            raise MandatoryParamError("Formal Graph")

        self.informal_graph = informal_graph
        self.formal_graph = formal_graph

        self.logger.info("\n\nBuild Sentiment Analysis")

    def process(self) -> json:

        # combine formal & informal graphs
        sentiment_analysis_graph = nx.compose(self.informal_graph, self.formal_graph)

        # add edge attribute weight
        nx.set_edge_attributes(sentiment_analysis_graph, 1, 'weight')

        # add betweenness centrality attribute
        betweenness_centrality = nx.betweenness_centrality(sentiment_analysis_graph)
        nx.set_node_attributes(sentiment_analysis_graph, betweenness_centrality, 'betweenness_centrality')

        # convert to json
        sentiment_analysis_json = json_graph.node_link_data(sentiment_analysis_graph)

        # create json file
        # Note: for testing
        # with open('T_graph.json', 'w') as outfile:
        #     json.dump(sentiment_analysis_json, outfile)

        self.logger.info("\n\nSentiment Analysis Successfully Completed.")

        return sentiment_analysis_json
