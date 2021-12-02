#!/usr/bin/env python
# -*- coding: UTF-8 -*-


import os
import time
from typing import Optional

import pandas as pd
from pandas import DataFrame

from base import BaseObject


class TransformCendantRecords(BaseObject):
    """ Service to Transform a Cendant Collection   """

    def __init__(self):
        """
        Created:
            20-Jan-2020
            craig.trim@ibm.com
            *   https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1769
        """
        BaseObject.__init__(self, __name__)

    @classmethod
    def to_list(cls,
                a_record: dict,
                tags_as_csv: bool = True,
                include_text: bool = True) -> list:
        results = []

        for field in a_record["fields"]:

            def total_tags() -> int:
                if "tags" in field:
                    if "supervised" in field["tags"]:
                        return len(field["tags"]["supervised"])
                return 0

            def tags() -> list:
                if "tags" in field:
                    if "supervised" in field["tags"]:
                        return field["tags"]["supervised"]
                return []

            def tag_list() -> str:
                return ', '.join([x[0] for x in tags()])

            def normalized() -> Optional[str]:
                if "normalized" in field:
                    return ' '.join(field["normalized"])

            def value() -> str:
                return ' '.join([str(x) for x in field['value']])

            d_record = {
                "Id": a_record["key_field"],
                "Agent": field["agent"],
                "Type": field["type"],
                "Name": field["name"],
                "CollectionName": field['collection']['name'],
                "CollectionType": field['collection']['type']}

            if not tags_as_csv:

                for tag in tags():
                    d_record["Tag"] = tag[0]
                    d_record["Confidence"] = tag[1]

                    if include_text:
                        d_record["Value"] = value()
                        d_record["Normalized"] = normalized()

                    results.append(d_record)

            else:
                d_record["TagList"] = tag_list()
                d_record["TotalTags"] = total_tags()

                if include_text:
                    d_record["Value"] = value()
                    d_record["Normalized"] = normalized()

                results.append(d_record)

        return results

    @classmethod
    def to_dataframe(cls,
                     a_record: dict,
                     tags_as_csv: bool = True,
                     include_text: bool = True) -> DataFrame:
        return pd.DataFrame(cls.to_list(a_record=a_record,
                                        tags_as_csv=tags_as_csv,
                                        include_text=include_text))

    @classmethod
    def to_file(cls,
                records: list,
                file_name: str,
                tags_as_csv: bool = True,
                include_text: bool = True) -> None:
        """
        Purpose:
            Write a list of Cendant Records to file
        :param records:
            a list of one-or-more Cendant rec ords
        :param file_name:
            the name of the file to write
        :param tags_as_csv:
            True        transform all tags to a CSV list and write to a single column
            False       each tag gets a new row in the DataFrame
                        WARNING     this results in a highly denormalized DataFrame
                                    with a lot of records
        :param include_text:
            True        include the Original and Normalized text in the Dataframe
            False       no text is included
        """
        start = time.time()

        ctr = 0
        master = []

        for record in records:
            master += cls.to_list(record,
                                  tags_as_csv=tags_as_csv,
                                  include_text=include_text)

            ctr += 1
            if ctr % 1000 == 0:
                print(f"Progress {ctr}-{len(records)}")

        def _outfile() -> str:
            outpath = os.path.join(os.environ["CODE_BASE"],
                                   f"resources/output/collections")
            if not os.path.exists(outpath):
                os.makedirs(outpath)

            return os.path.join(outpath,
                                f"{file_name}.csv")

        outfile = _outfile()
        df_collection = pd.DataFrame(master)
        df_collection.to_csv(outfile, encoding="utf-8", sep="\t")

        print('\n'.join([
            f"Wrote Records to File",
            f"\tFile Path: {outfile}",
            f"\tTotal Records: {len(df_collection)}",
            f"\tTotal Time: {round(time.time() - start, 1)}s"]))
