#!/usr/bin/env python
# -*- coding: UTF-8 -*-


import pprint
from random import randint

from base import BaseObject
from base import MandatoryParamError


class IngestManifestData(BaseObject):
    """ ingest data from a provenance manifest """

    def __init__(self,
                 some_manifest_name: str,
                 some_activity_name: str):
        """
        Created:
            7-Mar-2019
            craig.trim@ibm.com
        Updated:
            26-June-2019
            abhbasu3@in.ibm.com
            *   db2 auto ingestion
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/370
        Updated:
            25-July-2019
            abhbasu3@in.ibm.com
            *   db2 auto ingestion & file system ingestion working together
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/370
        Updated:
            12-September-2019
            abhbasu3@in.ibm.com
            *   generate missing cities, countries csv
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/863
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

    def process(self,
                flush_target_records=True):
        from dataingest.core.dmo import IngestDataRules
        from dataingest.core.dmo import IngestFieldMapping
        from dataingest.core.dmo import IngestDataTransform
        from dataingest.core.dmo import ManifestActivityFinder
        from dataingest.core.dmo import ManifestConnectorForMongo
        from dataingest.core.dmo import SQLQueryReader
        from dataingest.core.dmo import IngestDataExtractorDB
        from dataingest.core.dmo import IngestDataExtractor
        from datadb2.core.bp import DBCredentialsAPI
        from cendalytics.geo.svc import GenerateMissingGeo

        d_manifest = ManifestActivityFinder(self.manifest_name,
                                            self.activity_name).process()

        # fetch datasource
        # datasource, datasource_details = FetchDataSource(d_manifest).process()

        df_src = None

        if "database" in d_manifest["source"]:
            datasource_details = d_manifest["source"]["database"]
            # get db credentials
            db2username, db2password = DBCredentialsAPI(datasource_details["username"],
                                                    datasource_details["password"]).process()
                                                            # Fetch SQL query
            sql_query = SQLQueryReader(self.manifest_name, self.activity_name).process()

            # fetch dataset
            df_src = IngestDataExtractorDB(self.activity_name,
                                        sql_query,
                                        db2username,
                                        db2password,
                                        datasource_details["hostname"],
                                        datasource_details["dbname"],
                                        datasource_details["port"]).process()

        elif "filesystem" in d_manifest["source"]:
             df_src = IngestDataExtractor(d_manifest).process()
             
        else:
            df_src = IngestDataExtractor(d_manifest).process()
            ##raise NotImplementedError("Only Database Ingestions are Supported")

        IngestDataRules().process(d_manifest, df_src)

        target_records = IngestFieldMapping(d_manifest,
                                            df_src).process()

        target_records = IngestDataTransform(target_records).process()

        target_collection = ManifestConnectorForMongo(d_manifest["target"]).process()

        if flush_target_records:
            target_collection.delete()

        target_collection.insert_many(target_records)

        # auto-generate missing geo city and country csv
        if self.activity_name == "OpenSeats":
            self.logger.debug("Generate Missing Cities CSV")
            GenerateMissingGeo.generate_missing_data("city")
            self.logger.debug("Generate Missing Countries CSV")
            GenerateMissingGeo.generate_missing_data("country")

        random_record = randint(0, len(target_records) - 1)
        self.logger.debug("\n".join([
            "Manifest Ingestion Activity Completed",
            "\tName: {}".format(self.activity_name),
            "\tRandom Record({}):".format(random_record),
            pprint.pformat(target_records[random_record], indent=4)]))
