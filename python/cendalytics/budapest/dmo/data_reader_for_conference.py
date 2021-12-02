# !/usr/bin/env python
# -*- coding: UTF-8 -*-


import os

from pandas import DataFrame

from base import BaseObject
from dataingest import ExcelReader


class DataReaderForConference(BaseObject):
    """
    """

    _columns = ['course_code',
                'course_name',
                'class_code',
                'class_start',
                'last_name',
                'first_name',
                'email',
                'external',
                'enrolled_status',
                'enrolled_date',
                'waitlist_position',
                'apr_status',
                'apr_date',
                'seat_held',
                'audience',
                'invitation_status',
                'invitation_date',
                'attendance_status',
                'walk_in',
                'duration',
                'price_quoted',
                'price_charged',
                'billing_info',
                'cancel_date',
                'cancel_type',
                'cancel_reason',
                'cancel_text',
                'cmp_status',
                'cmp_date',
                'needs',
                'cnum',
                'notes_id',
                'country_code',
                'department',
                'division',
                'organization',
                'group',
                'ebu_code',
                'geography',
                'region',
                'country_name',
                'job_role_id',
                'job_role_name',
                'cic',
                'bu_code',
                'bu_name',
                'lob_code',
                'lob_name',
                'manager_name',
                'manager_email',
                'approver_name',
                'approver_email']

    def __init__(self):
        """
        Created:
            4-Apr-2019
            craig.trim@ibm.com
        """
        BaseObject.__init__(self, __name__)

    @staticmethod
    def _path():
        return os.path.join(os.environ["GTS_BASE"],
                            "resources/confidential_input/budapest/data.xlsx")

    def process(self) -> DataFrame:
        d_columns = dict((key, 'str') for (key) in self._columns)
        df = ExcelReader.read_excel(some_input_path=self._path(),
                                    skiprows=1,
                                    usecols="A:AZ",
                                    column_names=d_columns)

        self.logger.debug("\n".join([
            "Read Spreadsheet into Pandas Dataframe",
            "\tlen: {}".format(len(df))
        ]))

        return df


if __name__ == "__main__":
    ConferenceDataReader().process()
