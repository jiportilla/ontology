#!/usr/bin/env python
# -*- coding: UTF-8 -*-


from base import BaseObject
from base import MandatoryParamError


class SocialNetworkOrchestrator(BaseObject):
    """ Orchestrate Social NetWork Analysis """

    def __init__(self,
                 blog_url):
        """
        Created:
            - 13-July-2019
            - abhbasu3@in.ibm.com
        """
        BaseObject.__init__(self, __name__)
        if not blog_url:
            raise MandatoryParamError("Blog URL")

        self.blog_url = blog_url

    def process(self):
        from cendalytics.sna import SocialNetworkDataExtractor
        from cendalytics.sna import FetchBlogInterestUsers
        from cendalytics.sna import BuildInformalSocialNetwork
        from cendalytics.sna import BuildFormalSocialNetwork
        from cendalytics.sna import SentimentAnalysis

        # extract data from db2
        df_src = SocialNetworkDataExtractor().process()

        # sort by compound score
        df_src = df_src.sort_values(by='COMPOUND_SCORE', ascending=True)
        # add rank column
        df_src['RANK'] = range(1, 1 + len(df_src))

        # fetch users data interested in blog url
        df_interested_users = FetchBlogInterestUsers(df_src, self.blog_url).process()

        # fetch list of unique interested user emails for the social network
        list_of_interested_user_emails = df_interested_users['AUTHOR_EMAIL'].unique().tolist()

        # build informal graph
        informal_social_network_graph = BuildInformalSocialNetwork(df_interested_users).process()

        # build formal network graph
        formal_social_network_graph = BuildFormalSocialNetwork(list_of_interested_user_emails).process()

        # return informal_social_network_graph, formal_social_network_graph

        sentiment_analysis = SentimentAnalysis(informal_social_network_graph, formal_social_network_graph).process()

        return sentiment_analysis
