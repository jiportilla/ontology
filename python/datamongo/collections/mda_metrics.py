#!/usr/bin/env python
# -*- coding: UTF-8 -*-


from base import BaseObject
from base import FileIO
from base import StringIO
from datamongo.core.bp import CendantCollection
from datamongo.core.dmo import BaseMongoClient


class MdaMetrics(BaseObject):
    """ Collection Wrapper over MongoDB Collection for 'mda_metrics'  """

    _records = []

    def __init__(self,
                 some_base_client=None):
        """
        Created:
            18-Apr-2019
            craig.trim@ibm.com
            *   https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/60
        """
        BaseObject.__init__(self, __name__)
        if not some_base_client:
            some_base_client = BaseMongoClient()

        self.collection = CendantCollection(some_base_client=some_base_client,
                                            some_db_name="cendant",
                                            some_collection_name="metrics_mda")

    def _all(self) -> list:
        if not self._records:
            self._records = self.collection.all()
        return self._records

    def to_csv(self,
               file_out_path: str):

        lines = [
            "Timestamp, Labels, "
            "Ngram Keys, Ngram Values,"
            "Parent Keys, Parent Values,"
            "Pattern Keys, Pattern Values,"
            "Defines Keys, Defines Values,"
            "Implies Keys, Implies Values,"
            "Infinitive Keys, Infinitive Values,"
            "Owns Keys, Owns Values,"
            "Parts Keys, Parts Values,"
            "Requires Keys, Requires Values,"
            "Similarity Keys, Similarity Values,"
            "Version Keys, Version Values"]

        d_metrics = {}
        for record in self._all():
            values = []

            rels = dict(record["rels"])
            entities = dict(record["entities"])

            def _to_date():
                d = StringIO.to_date(record["tts"])
                return d.split(" ")[0].strip()

            _date = _to_date()
            values.append(_date)

            values.append(entities["labels"]["keys"])
            values.append(entities["ngrams"]["keys"])
            values.append(entities["ngrams"]["values"])
            values.append(entities["parents"]["keys"])
            values.append(entities["parents"]["values"])
            values.append(entities["patterns"]["keys"])
            values.append(entities["patterns"]["values"])

            values.append(rels["defines"]["keys"])
            values.append(rels["defines"]["values"])
            values.append(rels["implies"]["keys"])
            values.append(rels["implies"]["values"])
            values.append(rels["infinitive"]["keys"])
            values.append(rels["infinitive"]["values"])
            values.append(rels["owns"]["keys"])
            values.append(rels["owns"]["values"])
            values.append(rels["parts"]["keys"])
            values.append(rels["parts"]["values"])
            values.append(rels["requires"]["keys"])
            values.append(rels["requires"]["values"])
            values.append(rels["similarity"]["keys"])
            values.append(rels["similarity"]["values"])
            values.append(rels["versions"]["keys"])
            values.append(rels["versions"]["values"])

            values = [str(x) for x in values if x]
            d_metrics[_date] = values

        for v in list(d_metrics.values()):
            lines.append(", ".join(v))

        FileIO.lines_to_file(lines,
                             file_out_path)


if __name__ == "__main__":
    MdaMetrics().to_csv("/Users/craigtrim/Desktop/mda-metrics.csv")
