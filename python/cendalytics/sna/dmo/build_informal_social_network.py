#!/usr/bin/env python
# -*- coding: UTF-8 -*-


import networkx as nx
from pandas import DataFrame

from base import BaseObject


# from base import MandatoryParamError

class BuildInformalSocialNetwork(BaseObject):
    """ Orchestrate Social NetWork Analysis """

    def __init__(self,
                 interested_users_df: DataFrame,
                 prod_url="w3-connections.ibm.com"):
        """
        Created:
            - 15-July-2019
            - abhbasu3@in.ibm.com
        """
        BaseObject.__init__(self, __name__)

        self.interested_users_df = interested_users_df
        self.prod_url = prod_url

        self.informal_graph = nx.MultiDiGraph()

        self.logger.info("\n\nBuild informal graph")

    # Build network for a user
    def build_informal_network_for_user(self, user_email, content_df):
        from cendalytics.sna import CheckUserConnections

        # Add node for original user
        self.informal_graph.add_node(user_email)

        # check connections for users
        checkconnection = CheckUserConnections()
        connections_email_list = checkconnection.get_connections(self.prod_url, user_email)

        if connections_email_list:
            # Add nodes for connections
            for i in connections_email_list:
                self.informal_graph.add_node(i, type="USER")

                # Add connections between user and connections
                if i != user_email:
                    # add_edges_for_user(user_email,connections_email_list,G,'social')
                    self.informal_graph.add_edge(user_email, i, type='social')

        # add comment nodes & edges
        user_content_df = content_df.loc[content_df["AUTHOR_EMAIL"] == user_email]
        for rank in user_content_df['RANK']:
            self.informal_graph.add_node(rank)
            self.informal_graph.add_edge(user_email, rank, type='created')

    def process(self) -> DataFrame:

        # users of interest
        users_of_interest = self.interested_users_df.filter(['AUTHOR_EMAIL', 'COMPOUND_SCORE'], axis=1)
        users_of_interest_dict = dict(users_of_interest.values.tolist())

        # get content
        content_df = self.interested_users_df.filter(['AUTHOR_EMAIL', 'RANK'], axis=1)

        for element in users_of_interest_dict:
            self.build_informal_network_for_user(element, content_df)

        print("Informal no. of edges:  ", self.informal_graph.number_of_edges())
        print("Informal no. of nodes:  ", self.informal_graph.number_of_nodes())

        # Set degree attribute for social connections
        for node, data in self.informal_graph.nodes(data=True):
            social_degree = self.informal_graph.degree[node]
            nx.set_node_attributes(self.informal_graph, social_degree, 'social_degree')

        # get comment
        comment_df = self.interested_users_df.filter(['RANK', 'CONTENT'], axis=1)
        comment_df_dict = dict(comment_df.values.tolist())
        # add comment attribute
        nx.set_node_attributes(self.informal_graph, comment_df_dict, 'content')

        # get modify date
        comment_post_date = self.interested_users_df.filter(['RANK', 'MODIFY_DATE'], axis=1)
        comment_post_date['MODIFY_DATE'] = comment_post_date['MODIFY_DATE'].astype(str)
        comment_post_date_dict = dict(comment_post_date.values.tolist())
        # add date attribute
        nx.set_node_attributes(self.informal_graph, comment_post_date_dict, 'comment_date')

        # add type
        type_df_comment = self.interested_users_df.filter(['RANK'], axis=1)
        type_df_user = self.interested_users_df.filter(['AUTHOR_EMAIL'], axis=1)
        type_df_comment["TYPE"] = "COMMENT"
        type_df_user["TYPE"] = "USER"
        type_df_comment_dict = dict(type_df_comment.values.tolist())
        type_df_user_dict = dict(type_df_user.values.tolist())
        nx.set_node_attributes(self.informal_graph, type_df_user_dict, "type")
        nx.set_node_attributes(self.informal_graph, type_df_comment_dict, "type")

        # get score
        score_df = self.interested_users_df.filter(['RANK', 'COMPOUND_SCORE'], axis=1)
        default_score_df = self.interested_users_df.filter(['RANK'], axis=1)
        default_score_df["DEFAULT_SCORE"] = '-99'
        default_score_df_dict = dict(default_score_df.values.tolist())
        score_df_dict = dict(score_df.values.tolist())
        # Set COMPOUND SCORE as node attribute for users who have commented on blog
        # nx.set_node_attributes(self.informal_graph, -99, 'score')  # set default score
        nx.set_node_attributes(self.informal_graph, default_score_df_dict, 'score')  # set default score
        nx.set_node_attributes(self.informal_graph, score_df_dict, 'score')

        self.logger.info("\n\nInformal Graph Created.")

        return self.informal_graph
