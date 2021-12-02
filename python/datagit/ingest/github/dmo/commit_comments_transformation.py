#!/usr/bin/env python
# -*- coding: UTF-8 -*-


from base import BaseObject
from base import DataTypeError
from base import FieldStructure
from base import MandatoryParamError
from base import RecordStructure


class CommitCommentTransformation(BaseObject):
    """ Transform GitHub Commit Comment into the Cendant Field Structure """

    def __init__(self,
                 issue_id: int or str,
                 parent_id: str,
                 manifest_name: str,
                 repo_name: str,
                 svcresult: dict,
                 is_debug: bool = False):
        """
        Created:
            6-Dec-2019
            craig.trim@ibm.com
            *   https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1540
        :param issue_id:
            this is a provenance link to the source issue
            in this case it would be the original GitHub Issue ID the comments are linked to
        :param manifest_name:
            this is a provenance link to the source data
            in this case it would be the original GitHub URL the data was taken from
        :param repo_name:
            the name of the repository the data is drawn from
        :param svcresult:
            an event from GitHub
        :param is_debug:
        """
        BaseObject.__init__(self, __name__)

        if type(svcresult) != dict:
            raise DataTypeError("Service Result, dict")

        if not issue_id:
            raise MandatoryParamError("Issue ID")

        if not parent_id:
            raise MandatoryParamError("Parent ID")

        self._issue_id = issue_id
        self._is_debug = is_debug
        self._parent_id = parent_id
        self._svcresult = svcresult
        self._repo_name = repo_name
        self._manifest_name = manifest_name

    def _record_id(self) -> str:
        return f"{self._parent_id}-{self._svcresult['node_id']}"

    @staticmethod
    def _field(field_type: str,
               field_name: str,
               field_value: str) -> dict:
        return FieldStructure.generate_src_field(agent_name="System",
                                                 field_type=field_type,
                                                 field_name=field_name,
                                                 field_value=field_value,
                                                 collection="Comments")

    def _commit_record(self,
                       event: dict) -> dict:
        fields = []

        def body():
            return event["body"].strip()

        def created_at() -> str:
            return event["created_at"].split('T')[0].strip()

        def updated_at() -> str:
            return event["updated_at"].split('T')[0].strip()

        def html_url() -> str:
            return str(event["html_url"])

        fields.append(self._field(field_type="long-text",
                                  field_name="Body",
                                  field_value=body()))

        fields.append(self._field(field_type="date",
                                  field_name="OpenBy",
                                  field_value=created_at()))

        fields.append(self._field(field_type="date",
                                  field_name="UpdateDate",
                                  field_value=updated_at()))

        fields.append(self._field(field_type="url",
                                  field_name="HtmlURL",
                                  field_value=html_url()))

        return RecordStructure.github_record(fields=fields,
                                             div_field="commit-comment",
                                             key_field=self._record_id(),
                                             key_field_parent=self._parent_id,
                                             issue_id=self._issue_id,
                                             repo_name=self._repo_name,
                                             manifest_name=self._manifest_name)

    def process(self) -> list:
        records = [self._commit_record(self._svcresult)]
        records = [x for x in records if x]

        if self._is_debug:
            self.logger.debug('\n'.join([
                "Commit Comment Tranformation Complete",
                f"\tIssue #{self._issue_id}",
                f"\tTotal Records: {len(records)}"]))

        return records
