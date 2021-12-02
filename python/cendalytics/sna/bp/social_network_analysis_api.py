#!/usr/bin/env python
# -*- coding: UTF-8 -*-


from base import BaseObject
from base import MandatoryParamError


class SocialNetworkAnalysisAPI(BaseObject):
    """ API to Persist Data in DB2 """

    def __init__(self):
        """
        Created:
            - 13-July-2019
            - abhbasu3@in.ibm.com
        """
        BaseObject.__init__(self, __name__)

    @staticmethod
    def socialnetworkanalysis(url, intranet_id, intranet_pass):
        from cendalytics.sna import SocialNetworkOrchestrator
        from cendalytics.sna import InputCredentials

        # set credentials
        InputCredentials.set_config_details(intranet_id, intranet_pass)

        social_network_api = SocialNetworkOrchestrator(url)
        sentiment_analysis = social_network_api.process()

        return sentiment_analysis

    @staticmethod
    def process(blog_url, username='', password='', db2username='', db2password=''):

        import getpass

        try:
            print("Enter credentials:\n")
            if not username:
                username = input("Intranet ID: ")
            if not password:
                password = getpass.getpass(prompt='Your Intranet password: ')
            # db2username = input("DB2 Username: ")
            # db2password = getpass.getpass(prompt='DB2 Password: ')
        except Exception as err:
            print('ERROR:', err)
        else:
            print("\n".join([
                "API Parameters",
                "\tIntranet-id: {}".format(username)]))

        print("\n".join([
            "API Parameters",
            "\tblog_url: {}".format(blog_url)
        ]))

        if not username:
            raise MandatoryParamError("Intranet id")
        if not password:
            raise MandatoryParamError("Intranet Password")
        # if not db2username:
        #     raise MandatoryParamError("DB2 Username")
        # if not db2password:
        #     raise MandatoryParamError("DB2 Password")

        if blog_url.startswith("http"):
            sentiment_analysis_json = SocialNetworkAnalysisAPI.socialnetworkanalysis(blog_url,
                                                                                     username,
                                                                                     password)
        else:
            raise ValueError("\n".join([
                "Unrecognized Input",
                f"\tname: {blog_url}"]))

        return sentiment_analysis_json


if __name__ == "__main__":
    sentiment_data = SocialNetworkAnalysisAPI.process(
        "https://w3-connections.ibm.com/blogs/0d86cb37-869b-435e-8ffc-f2f16949d5ee/entry/GTS_Academy?lang=en_us")
