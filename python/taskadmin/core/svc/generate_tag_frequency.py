#!/usr/bin/env python
# -*- coding: UTF-8 -*-

import codecs
import os
import time
from collections import Counter

from base import BaseObject
from base import MandatoryParamError
from datamongo import CendantCollection


class GenerateTagFrequency(BaseObject):
    """ """

    def __init__(self,
                 some_collection_name: str):
        """
        Updated:
            20-Mar-2019
            craig.trim@ibm.com
            *   moved out of a 'scripts' directory and cleaned up
        """
        BaseObject.__init__(self, __name__)
        if not some_collection_name:
            raise MandatoryParamError("Collection Name")
        self.collection_name = some_collection_name

    def _write_to_file(self,
                       supervised: Counter,
                       unsupervised: Counter):

        def _output_path():
            return os.path.join(os.environ["DESKTOP"],
                                "{}-tag-frequency-{}.csv".format(self.collection_name,
                                                                 str(time.time())))

        target = codecs.open(_output_path(),
                             mode="w",
                             encoding="utf-8")

        def _write(c: Counter,
                   label: str):
            for x in c:
                target.write("{}\t{}\t{}\n".format(x,
                                                   c[x],
                                                   label))

        _write(supervised, "supervised")
        _write(unsupervised, "unsupervised")

        target.close()

    def process(self,
                write_to_file=True) -> dict:

        collection = CendantCollection(some_db_name="cendant",
                                       some_collection_name=self.collection_name)
        all_records = collection.all()
        self.logger.debug("\n".join([
            "Retrieved Records",
            "\tcollection-name: {}".format(self.collection_name),
            "\ttotal-records: {}".format(len(all_records))
        ]))

        supervised = Counter()
        unsupervised = Counter()

        for record in all_records:
            for field in record["fields"]:
                if "tags" not in field:
                    continue
                for tag in field["tags"]["supervised"]:
                    supervised.update({tag: 1})
                for tag in field["tags"]["unsupervised"]:
                    unsupervised.update({tag: 1})

        if write_to_file:
            self._write_to_file(supervised,
                                unsupervised)

        return {
            "supervised": supervised,
            "unsupervised": unsupervised
        }


def main(collection):
    GenerateTagFrequency(collection).process(write_to_file=False)


if __name__ == "__main__":
    import plac

    plac.call(main)
