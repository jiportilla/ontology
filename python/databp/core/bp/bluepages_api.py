#!/usr/bin/env python
# -*- coding: UTF-8 -*-

from base import BaseObject
from databp.core.svc import ExportMappedEvents
from databp.core.svc import MapRawEvents
from databp.core.svc import QueryBluepagesEndpoint
from datamongo import BaseMongoClient
from datamongo import CendantCollection

import requests
import json

class BluepagesAPI(BaseObject):
    """
    Created:
        28-Nov-2018
        craig.trim@ibm.com
    """

    @staticmethod
    def by_internet_address(some_internet_address: str,
                            filter_empty=False) -> dict:
        return QueryBluepagesEndpoint().by_internet_address(some_internet_address=some_internet_address,
                                                            filter_empty=filter_empty,
                                                            use_cache=True)

    @staticmethod
    def by_internet_address_bulk(some_internet_addresses: list) -> int:
        return QueryBluepagesEndpoint().by_internet_address_bulk(some_internet_addresses)

    @staticmethod
    def map_raw_events():
        return MapRawEvents().process()

    @staticmethod
    def export_mapped_events():
        return ExportMappedEvents().process()

    @staticmethod
    def ingest_complete_collection():
        """"
            Created:
                25-Feb-2019
                thomasb@ie.ibm.com
                https://github.ibm.com/-CDO/unstructured-analytics/issues/1129
            Description:
                THe Bluepages API is queried and returns a JSON document which is then loaded into the
                "ingest_bluepages_api" collection in Mongodb, THe bluepages api details are in below swagger doc
                https://testpcrwbluetapvip01.w3-969.ibm.com/myw3/unified-profile/v2/api/#/default/get_profiles_sync
        """

        APIurl = "http://w3-services1.w3-969.ibm.com/myw3/unified-profile/v2/profiles/sync?format=custom&user_profile" \
                 "=uid,org,nameFull,callupName,employeeType,mail,role&user_expertise=" \
                 "expertiseSummary,resume,patents,publications,organizations,jobRoles"

        cloud_col = CendantCollection(some_db_name="cendant", some_collection_name="ingest_bluepages_api",
                                      some_base_client=BaseMongoClient(server_alias="CLOUD"))

        print(f"Executing API call with \n {APIurl}")
        r = requests.get(APIurl, stream=True)

        if r.status_code == 200:

            APICollection = json.loads(r.content)

            print("Deleting the existing collection data")
            cloud_col.delete()

            print("Inserting API results to ingest_bluepages_api collection")
            cloud_col.insert_many(APICollection)

            print(f"Number of records persisted {len(APICollection)}")
        else:
            raise Exception(f"Bluepages API returned response {r.status_code}, exiting ingest sequence")


def main(api_method):
    if api_method == "map":
        BluepagesAPI.map_raw_events()
    elif api_method == "export":
        BluepagesAPI.export_mapped_events()
    elif api_method == "all":
        BluepagesAPI.ingest_complete_collection()
    else:
        raise NotImplementedError("\n".join([
            "API Method Not Supported",
            "\t{0}".format(api_method)
        ]))


if __name__ == "__main__":
    import plac

    plac.call(main)
