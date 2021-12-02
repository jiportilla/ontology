import os.path

from base import BaseObject
from base import MandatoryParamError


class SQLQueryReader(BaseObject):
    """ Prepare SQL Queries """

    def __init__(self,
                 some_manifest_name: str,
                 some_activity_name: str):
        """
        Created:
            - 26-June-2019
            - abhbasu3@in.ibm.com
            - prepare sql query
            - Reference: <https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/370>
        Updated:
            - 27-July-2019
            - abhbasu3@in.ibm.com
            - added condition for gbs &  cv data ingestion
        :param some_manifest_name:
            the name of the ingestion activity
        :param some_activity_name:
        Updated:
            06-September-2019
            abhbasu3@in.ibm.com
            * added cloud ingestion
            * Reference: https://github.ibm.com/-cdo/unstructured-analytics/issues/869
        Updated:
            14-Oct-2019
            craig.trim@ibm.com
            *   renamed from 'prepare-ingestion-sql-query'
            Rationale:
                microservice naming standard is
                1.  DMO components to be 'NOUN-VERB'
                2.  SVC components to be 'VERB-NOUN'
        Updated:
            17-October-2019
            abhbasu3@in.ibm.com
            * added CHQ/OTH ingestion
            * Reference: https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1135
        Updated:
            29-October-2019
            abhbasu3@in.ibm.com
            * added Security BU ingestion
            * Reference: https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1182
        Updated:
            4-November-2019
            abhbasu3@in.ibm.com
            * added GLMKT BU ingestion
            * Reference: https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1245
        Updated:
            14-November-2019
            abhbasu3@in.ibm.com
            * added Watson Health BU ingestion
            * Reference: https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1348
        Updated:
            19-November-2019
            abhbasu3@in.ibm.com
            * added Research BU ingestion
            * Reference: https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1395
        Updated:
            03-December-2019
            abhbasu3@in.ibm.com
            * added Digital Sales BU ingestion
            * Reference: https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1497
        Updated:
            23-December-2019
            abhbasu3@in.ibm.com
            * added Industry Plat BU ingestion
            * Reference: https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1638
        Updated:
            21-Jan-2019
            abhbasu3@in.ibm.com
            * added GF BU ingestion
            * Reference: https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1773
        """
        BaseObject.__init__(self, __name__)
        if not some_activity_name:
            raise MandatoryParamError("Activity")

        self.manifest_name = some_manifest_name
        self.activity = some_activity_name

    @staticmethod
    def read_sql(sql_file_name) -> str:
        path = os.path.join(os.environ["CODE_BASE"],
                            "resources/manifest/queries",
                            "{}.sql".format(sql_file_name))

        # read from sql file
        sql_file = open(path, 'r')
        sql_stmt = sql_file.read()
        sql_file.close()

        return sql_stmt

    def process(self) -> str:

        sql_file_name = self.activity.lower().replace(" ", "_")

        # add postfix gbs| | cloud| systems| chq/oth| f&o| security| glmkt| cognitive apps| watson health|
        # digital sales| research| industry plat for cv ingestion
        if self.manifest_name == "ingest-manifest-cv-gbs":
            sql_file_name = sql_file_name + "_gbs"
        if self.manifest_name == "ingest-manifest-cv-":
            sql_file_name = sql_file_name + "_"
        if self.manifest_name == "ingest-manifest-cv-cloud":
            sql_file_name = sql_file_name + "_cloud"
        if self.manifest_name == "ingest-manifest-cv-systems":
            sql_file_name = sql_file_name + "_systems"
        if self.manifest_name == "ingest-manifest-cv-fno":
            sql_file_name = sql_file_name + "_fno"
        if self.manifest_name == "ingest-manifest-cv-chq-oth":
            sql_file_name = sql_file_name + "_chq_oth"
        if self.manifest_name == "ingest-manifest-cv-security":
            sql_file_name = sql_file_name + "_security"
        if self.manifest_name == "ingest-manifest-cv-glmkt":
            sql_file_name = sql_file_name + "_glmkt"
        if self.manifest_name == "ingest-manifest-cv-cognitive-apps":
            sql_file_name = sql_file_name + "_cognitive_apps"
        if self.manifest_name == "ingest-manifest-cv-watson-health":
            sql_file_name = sql_file_name + "_watson_health"
        if self.manifest_name == "ingest-manifest-cv-research":
            sql_file_name = sql_file_name + "_research"
        if self.manifest_name == "ingest-manifest-cv-digital-sales":
            sql_file_name = sql_file_name + "_digital_sales"
        if self.manifest_name == "ingest-manifest-cv-industry-plat":
            sql_file_name = sql_file_name + "_industry_plat"
        if self.manifest_name == "ingest-manifest-cv-gf":
            sql_file_name = sql_file_name + "_gf"

        sql_query = self.read_sql(sql_file_name)

        # add unrestricted access to db
        if "with ur" not in sql_query.lower():
            sql_query = f"{sql_query} WITH UR"
            self.logger.debug('\n'.join([
                "SQL Query Modified to add Unrestricted Access (UR)",
                sql_query]))

        if sql_query is None:
            raise NotImplementedError("SQL Query not found")

        return sql_query
