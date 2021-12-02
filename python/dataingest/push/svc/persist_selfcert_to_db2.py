import datetime as dt
import time

import plac
from pytz import timezone

from base import BaseObject
from base import DataTypeError
from base import MandatoryParamError
from datadb2.core.bp import DBCredentialsAPI
from dataingest.push.dmo import PersistDatatoDB
from cendalytics.skills.core.bp import SkillsReportAPI
from datadb2 import BaseDB2Client


def main(tag_collection_name):
    """
            Created:
                15-Oct-2019
                thomasb@ie.ibm.com
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1086
            Updated:
                19-Feb-2019
                thomasb@ie.ibm.com
                *added check for df size being <= 10% of the destination table
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1844
            Updated:
                20-FEb-2020
                thomasb@ie.ibm.com
                *added baseobject slack notificationto resolve #1844

    :param tag_collection_name
        the name of the source collection to extract the data from
    """

    # fetch db2 credentials
    db2username, db2password = DBCredentialsAPI("WFT_USER_NAME", "WFT_PASS_WORD").process()

    schema = 'WFT'
    table = 'D_SELF_IDENTIFIED_CERTIFICATIONS'

    if not tag_collection_name:
        raise MandatoryParamError('tag_collection_name')
    if type(tag_collection_name) != str:
        raise DataTypeError('tag_collection_name, str')

    def db2_count():
        base_client = BaseDB2Client(some_username=db2username, some_password=db2password)
        sql_query = f"select count(*) from {schema}.{table} with UR;"
        num_rows = base_client.connection.execute(sql_query)
        return num_rows.first()[0]

    script_Start = time.time()
    date_format = '%Y-%m-%d %H:%M:%S'
    source_date = dt.datetime.now().astimezone(timezone('US/Central'))

    api = SkillsReportAPI()

    print('Calling SkillsReportAPI to export Self reported certifications to DF')

    df = api.self_reported_certifications(
        aggregate_data=False,
        exclude_vendors=[''],
        collection_name=tag_collection_name)

    print("Transforming retuned DF")
    df = df.drop(columns=['Normalized'])
    df.columns = map(str.upper, df.columns)
    df['WFT_LASTUPDTS'] = source_date.strftime(date_format)

    db_row = db2_count()

    if len(df.index) <= (db_row*0.9):
        message = f'SkillsReportAPI.self_reported_certifications for {tag_collection_name} is greater than 10% ' \
                  f'smaller than current DB row count writing to DB2 aborted, api call returned {len(df.index)} rows'
        BaseObject.slack_notification(message)
        raise Exception(message)
    else:
        print(f'Persisting data to {schema} Schema, {table} table')
        PersistDatatoDB(db2username, db2password).process(df, schema, table)
        BaseObject.slack_notification(f'Self Certification data successfully persisted to the {schema}.{table} table\n'
                                      f'{len(df.index)} rows have been inserted')

    script_End = time.time()
    print("Overall Script Duration: {:.2f} minutes".format((script_End - script_Start) / 60))


if __name__ == '__main__':
    plac.call(main)
