#!/usr/bin/env python
# -*- coding: UTF-8 -*-


from pandas import Series

from base import BaseObject


class SentimentRecordStructure(object):
    """  Common Row Structure for a Sentiment record """

    @classmethod
    def test_records(cls,
                     tags: list,
                     record_id: str = None) -> list:
        """
        Purpose:
            Create Multiple Records (one record per tag) for a single User (record id)
        :param tags:
            the tags
        :param record_id:
            str         the record id to use for all tags
            None        generate a record id to use for all tags
        :return:
            a list of records
        """

        def get_record_id():
            if not record_id:
                return BaseObject.generate_tts()
            return record_id

        the_record_id = get_record_id()
        return [cls.test_record(tag=tag, record_id=the_record_id) for tag in tags]

    @classmethod
    def test_record(cls,
                    tag: str,
                    record_id: str = None) -> dict:
        """
        Purpose:
            Generate a record for testing
        :param tag:
            the tag to use in the record
        :param record_id:
        :return:
            a record (dict)
        """

        def _record_id():
            if not record_id:
                return BaseObject.generate_tts()
            return record_id

        return cls.record(record_id=_record_id(),
                          tag=tag,
                          schema="Test",
                          tenure="Test",
                          region="Test",
                          country="Test",
                          leadership="Test")

    @classmethod
    def test_summary_record(cls,
                            record_id: str,
                            category: str,
                            tag: str):
        d_record = cls.test_record(tag=tag,
                                   record_id=record_id)
        d_record['Category'] = category
        return d_record

    @classmethod
    def record(cls,
               record_id: str,
               tag: str,
               schema: str,
               tenure: str,
               region: str,
               country: str,
               leadership: str) -> dict:
        return {
            "RecordID": record_id,
            "Tag": tag,
            "Schema": schema,
            "Tenure": tenure,
            "Region": region,
            "Country": country,
            "Leadership": leadership}

    @classmethod
    def deep_copy(cls,
                  row: Series) -> dict:
        return {
            "RecordID": row['RecordID'],
            "Tag": row['Tag'],
            "Schema": row['Schema'],
            "Tenure": row['Tenure'],
            "Region": row['Region'],
            "Country": row['Country'],
            "Leadership": row['Leadership']}
