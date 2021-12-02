#!/usr/bin/env python
# -*- coding: UTF-8 -*-


import networkx as nx
from pandas import DataFrame

from base import BaseObject


class BuildFormalSocialNetwork(BaseObject):
    """ Orchestrate Social NetWork Analysis """

    def __init__(self,
                 user_emails: list):
        """
        Created:
            - 15-July-2019
            - abhbasu3@in.ibm.com
        """
        BaseObject.__init__(self, __name__)

        self.user_emails = user_emails

        self.formal_graph = nx.MultiDiGraph()

        self.logger.info("\n\nBuild formal graph")

    def get_manager_email(self, data_df, user_email):
        if data_df.loc[data_df['EMAIL'] == user_email].empty:
            mgr_email = 'NO MANAGER'
        else:
            dict1 = data_df['MGR_EMAIL'].to_dict()
            mgr_email = list(dict1.values())[0]
        return mgr_email

    # def get_functional_manager_email():

    # Build network for a user
    def build_formal_network_for_user(self, manager_df, functional_manager_df, user_email):
        self.formal_graph.add_node(user_email)

        # get manager email
        mgr_email = self.get_manager_email(manager_df, user_email)

        # get functional manager email
        func_mgr_email = self.get_manager_email(functional_manager_df, user_email)

        # add nodes
        self.formal_graph.add_node(mgr_email)
        self.formal_graph.add_node(func_mgr_email)

        # add edges
        self.formal_graph.add_edge(user_email, mgr_email, type='Mgr')
        self.formal_graph.add_edge(user_email, func_mgr_email, type='Func_Mgr')

    def process(self) -> DataFrame:
        from cendalytics.sna import SocialNetworkDataExtractor

        manager_data_df = SocialNetworkDataExtractor().extract_manager_data()
        funcional_manager_data_df = SocialNetworkDataExtractor().extract_functional_manager_data()

        for email in self.user_emails:
            self.build_formal_network_for_user(manager_data_df, funcional_manager_data_df, email)

        # remove 'NO MANAGER' NODE
        if self.formal_graph.has_node('NO MANAGER'):
            self.formal_graph.remove_node('NO MANAGER')

        print("Formal no. of edges:  ", self.formal_graph.number_of_edges())
        print("Formal no. of nodes:  ", self.formal_graph.number_of_nodes())

        # Set default 'social_degree'
        social_degree = 0
        nx.set_node_attributes(self.formal_graph, social_degree, 'social_degree')

        self.logger.info("\n\nFormal Graph Created.")

        return self.formal_graph
