#!/usr/bin/env python
# -*- coding: UTF-8 -*-

import csv
import os

from base.core.dmo import BaseObject
from datamongo import CendantCollection


class ExportMappedEvents(BaseObject):
    """ exports raw events to CSV """

    def __del__(self):
        self.timer.log()

    def __init__(self):
        """
        Created:
            30-Nov-2018
            craig.trim@ibm.com
        """
        BaseObject.__init__(self, __name__)

        self.bp_api_raw_collection = CendantCollection(some_db_name="cendant",
                                                       some_collection_name="bluepages_raw")
        self.bp_api_mapped_collection = CendantCollection(some_db_name="cendant",
                                                          some_collection_name="bluepages_mapped")

    variables = ["serial_number",
                 "phone_number",
                 "first_name",
                 "last_name",
                 "department",
                 "employee_type",
                 "is_manager",
                 "job_responsibility",
                 "email_address",
                 "email_domain",
                 "building",
                 "office",
                 "work_location",
                 "country",
                 "manager_serial_number",
                 "employee_number",
                 "workplace_indicator",
                 "organization_code",
                 "hr_country_code",
                 "hr_employee_type",
                 "division",
                 "exists"]

    def process(self):

        def _key(some_record, name):
            if name in some_record:
                return str(some_record[name])
            return "None"

        records = self.bp_api_mapped_collection.all()

        csvfile = os.path.join(os.environ["OUTPUT_DIR"],
                               "bp-mapped-events.csv")

        with open(csvfile, 'w') as csvfile:
            writer = csv.writer(csvfile,
                                delimiter='\t',
                                quotechar='|',
                                quoting=csv.QUOTE_MINIMAL)

            writer.writerow(self.variables)
            for record in records:
                writer.writerow([_key(record, x) for x in self.variables])
