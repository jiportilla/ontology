#!/usr/bin/env python
# -*- coding: utf-8 -*-


from tabulate import tabulate

from base import BaseObject
from datamongo import BaseMongoClient
from datamongo import CendantCollection
from datamongo import CendantXdm
from datamongo import TransformCendantRecords


class RecordRetriever(BaseObject):

    def __init__(self,
                 key_field: str,
                 mongo_client: BaseMongoClient,
                 collection_names: dict,
                 is_debug: bool = False):
        """
        Created:
            13-Nov-2019
            craig.trim@ibm.com
            *   refactored out of 'generate-feedback-report'
        Updated:
            14-Nov-2019
            craig.trim@ibm.com
            *   added random record capability
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1331#issuecomment-16009658
        :param key_field:
            the value of the keyfield to process
            (e.g. the actual Serial Number or Open Seat ID)
        :param collection_names:
            a dictionary containing a complete set of collection names
            Sample Input:
                {   'src':  'supply_src_20191025',
                    'tag':  'supply_tag_20191025',
                    'xdm':  'supply_xdm_20191029' }
        :param is_debug:
        """
        BaseObject.__init__(self, __name__)

        self._is_debug = is_debug
        self._key_field = key_field
        self._mongo_client = mongo_client
        self._collection_names = collection_names

    def _retrieve_record(self,
                         collection_name: str):
        collection = CendantCollection(is_debug=self._is_debug,
                                       some_base_client=self._mongo_client,
                                       some_collection_name=collection_name)

        if self._key_field.lower().strip() == "random":  # GIT-1331-16009658
            return collection.random(total_records=1)[0]

        return collection.by_key_field(self._key_field)

    def _tag_record(self) -> dict or None:
        tag_record = self._retrieve_record(self._collection_names["tag"])

        if not self._collection_names["tag"]:
            self.logger.warning('\n'.join([
                "TAG Collection Not Found",
                f"\t{self._collection_names}"]))
            return None

        if not tag_record:
            self.logger.warning('\n'.join([
                f"TAG Record Not Found ("
                f"key-field={self._key_field})"]))
            return None

        if self._is_debug:
            df_record = TransformCendantRecords.to_dataframe(a_record=tag_record,
                                                             include_text=False)
            self.logger.debug('\n'.join([
                f"Retrieved TAG Record ("
                f"key-field={self._key_field})",
                tabulate(df_record, tablefmt='psql', headers='keys')]))

        return tag_record

    def _xdm_record(self) -> dict or None:

        if not self._collection_names["xdm"]:
            self.logger.warning('\n'.join([
                "XDM Collection Not Found",
                f"\t{self._collection_names}"]))
            return None

        xdm_record = self._retrieve_record(self._collection_names["xdm"])

        if not xdm_record:
            self.logger.warning('\n'.join([
                f"XDM Record Not Found ("
                f"key-field={self._key_field})"]))
            return None

        if self._is_debug:
            self.logger.debug('\n'.join([
                f"Retrieved XDM Record ("
                f"key-field={self._key_field})",
                tabulate(CendantXdm.dataframe(xdm_record),
                         tablefmt='psql',
                         headers='keys')]))

        return xdm_record

    def process(self) -> dict:

        svcresult = {
            "tag": self._tag_record(),
            "xdm": self._xdm_record()}

        if self._is_debug:
            self.logger.debug('\n'.join([
                "Records Retrieved",
                f"\tKey Field: {self._key_field}",
                f"\tTAG Collection: {self._collection_names['tag']}",
                f"\tXDM Collection: {self._collection_names['xdm']}"]))

        return svcresult
