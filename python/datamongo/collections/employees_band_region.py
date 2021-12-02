    #!/usr/bin/env python
# -*- coding: UTF-8 -*-


from base import BaseObject
from base import RecordUnavailableRecord
from datamongo.core.bp import CendantCollection
from datamongo.core.dmo import BaseMongoClient


class BandRegion(BaseObject):
    """ Collection Wrapper over MongoDB Collection
        for "_employees_band_region"   """

    def __init__(self,
                 some_base_client=None):
        """
        Created:
            16-Apr-2019
            craig.trim@ibm.com
        """
        BaseObject.__init__(self, __name__)
        if not some_base_client:
            some_base_client = BaseMongoClient()

        self.collection = CendantCollection(some_base_client=some_base_client,
                                            some_db_name="cendant",
                                            some_collection_name="ingest_band_region")

    @staticmethod
    def _reverse(some_d: dict) -> dict:
        d_reversed = {}
        for k in some_d:
            v = some_d[k]
            if v not in d_reversed:
                d_reversed[v] = []
            d_reversed[v].append(k)
        return d_reversed

    def all_region_by_cnum(self,
                           reverse=False):
        records = self.collection.all()

        d_index = {}
        ctr = 0
        for record in records:
            ctr += 1
            cnum = [x["value"] for x in record["fields"] if x["name"] == "serial_number"][0]
            value = [x["value"] for x in record["fields"] if x["name"] == "region"][0]
            if value:
                d_index[cnum] = value

        if reverse:
            return self._reverse(d_index)

        return d_index

    def all_band_by_cnum(self,
                         reverse=False):
        records = self.collection.all()

        d_index = {}
        for record in records:
            cnum = [x["value"] for x in record["fields"] if x["name"] == "serial_number"][0]
            value = [x["value"] for x in record["fields"] if x["name"] == "band"][0]
            if value:
                d_index[cnum] = value

        if reverse:
            return self._reverse(d_index)

        return d_index

    def _record_by_cnum(self,
                        some_serial_number: str,
                        raise_error: bool):
        record = self.collection.by_field("fields.value", some_serial_number)
        if not record:
            error = "\n".join([
                "Record Not Found (serial-number={})".format(
                    some_serial_number)])

            if raise_error:
                raise RecordUnavailableRecord(error)
            self.logger.error(error)

        return record

    def band_by_cnum(self,
                     some_serial_number: str,
                     raise_error: bool = True) -> int:
        record = self._record_by_cnum(some_serial_number,
                                      raise_error)
        if not record:
            return -1

        for field in record["fields"]:
            if field["name"] == "band":
                return int(field["value"])

    def region_by_cnum(self,
                       some_serial_number: str,
                       raise_error: bool = True) -> str:
        record = self._record_by_cnum(some_serial_number,
                                      raise_error)
        if not record:
            return "Unknown"

        for field in record["fields"]:
            if field["name"] == "region":
                return field["value"]
