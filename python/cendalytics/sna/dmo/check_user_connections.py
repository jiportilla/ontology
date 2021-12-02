#!/usr/bin/env python
# -*- coding: UTF-8 -*-


import base64
import http.client
import json

import numpy as np
import xmltodict

from base import BaseObject


class CheckUserConnections(BaseObject):
    """ Check User Connections on Social Network """

    def __init__(self,
                 connections_url: str = "w3-connections.ibm.com",
                 connections_api: str = "/profiles/atom/connections.do?email=",
                 connections_api_suffix: str = "&connectionType=colleague&status=accepted&output=vcard"):
        """
        Created:
            13-July-2019
            abhbasu3@in.ibm.com
        """
        BaseObject.__init__(self, __name__)

        from cendalytics.sna import InputCredentials

        # get intranet details
        self.intranet_username, self.intranet_passowrd = InputCredentials.getintranet()

        self.connections_url = connections_url
        self.connections_api = connections_api
        self.connections_api_suffix = connections_api_suffix

    # Transform list into dictionary
    def list_to_dict(self, dict_and_key):
        dict = {i: dict_and_key[i] for i in range(0, len(dict_and_key))}
        return dict

    def unique(self, list):
        x = np.array(list)
        return (np.unique(x))

    # Connect using BASIC OAUTH and get feed from IBM Connections via the APIs
    def get_connections(self, url, email):
        c = http.client.HTTPSConnection(url)
        userpass = self.intranet_username + ":" + self.intranet_passowrd
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:52.0) Gecko/20100101 Firefox/60.0',
                   'Content-type': 'application/atom+xml;charset=utf-8', \
                   'Authorization': 'Basic %s' % base64.b64encode(userpass.encode("ASCII")).decode("ASCII")}

        c.request("GET", self.connections_api + email + self.connections_api_suffix, "", headers)

        response = c.getresponse()

        # print(response.status, response.reason, response.getheaders())
        # print("\n=====================================\n")

        data = response.read()

        ordered_dict = xmltodict.parse(data)
        dict_format = dict(ordered_dict)

        try:
            feed = dict_format['feed']  # get main tag 'feed' from dictionary
            dump = json.dumps(feed)

            loads = json.loads(dump)

            entry = self.list_to_dict(loads["entry"])  # Get 'entry' tag, then transform resulting list into dictionary

            email_list = []

            for i in range(0, len(entry)):
                contributor = self.list_to_dict(entry[i]["snx:connection"]["contributor"])

                for j in range(0, len(contributor)):
                    email = contributor[j]["email"]
                    email_list.append(email)

            connections_email_list = list(filter(lambda a: a != email, email_list))
            connections_email_list = list(self.unique(connections_email_list))

            return connections_email_list

        except KeyError:
            pass
