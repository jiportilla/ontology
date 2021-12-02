#!/usr/bin/env python
# -*- coding: UTF-8 -*-


from base import BaseObject
from base import DataTypeError
from base import FieldStructure
from base import MandatoryParamError
from base import RecordStructure


class MentionsTransformation(BaseObject):
    """ Transform GitHub Mentions into the Cendant Field Structure """

    def __init__(self,
                 issue_id: int or str,
                 parent_id: str,
                 manifest_name: str,
                 repo_name: str,
                 svcresult: dict,
                 is_debug: bool = False):
        """
        Created:
            2-Dec-2019
            craig.trim@ibm.com
            *   https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1486
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

        if not parent_id or parent_id is None:
            raise MandatoryParamError("Parent ID")

        self._issue_id = issue_id
        self._parent_id = parent_id
        self._is_debug = is_debug
        self._svcresult = svcresult
        self._repo_name = repo_name
        self._manifest_name = manifest_name

    def _record_id(self) -> str:
        return f"{self._parent_id}-{self._svcresult['id']}"

    @staticmethod
    def _field(field_type: str,
               field_name: str,
               field_value: str) -> dict:
        return FieldStructure.generate_src_field(agent_name="System",
                                                 field_type=field_type,
                                                 field_name=field_name,
                                                 field_value=field_value,
                                                 collection="Comments")

    def process(self) -> list:
        records = []

        def created_at():
            return self._svcresult["created_at"].split('T')[0].strip()

        def mentioned():
            return self._svcresult["actor"]["login"].strip()

        def commit_url():
            return self._svcresult["commit_url"]

        fields = [
            self._field(field_type="user",
                        field_name="Mentioned",
                        field_value=mentioned()),

            self._field(field_type="url",
                        field_name="CommitURL",
                        field_value=commit_url()),

            self._field(field_type="text",
                        field_name="OpenDate",
                        field_value=created_at())]

        records.append(RecordStructure.github_record(fields=fields,
                                                     div_field="mentions",
                                                     issue_id=self._issue_id,
                                                     key_field=self._record_id(),
                                                     key_field_parent=self._parent_id,
                                                     repo_name=self._repo_name,
                                                     manifest_name=self._manifest_name))

        records = [x for x in records if x]
        if self._is_debug:
            self.logger.debug('\n'.join([
                "Mention Tranformation Complete",
                f"\tIssue #{self._issue_id}",
                f"\tTotal Records: {len(records)}"]))

        return records
