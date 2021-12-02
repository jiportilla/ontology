#!/usr/bin/env python
# -*- coding: UTF-8 -*-


from base import BaseObject
from base import DataTypeError
from base import FieldStructure
from base import MandatoryParamError
from base import RecordStructure


class PullRequestIssueTransformation(BaseObject):
    """ Transform GitHub Pull Request (PR) into the Cendant Field Structure """

    def __init__(self,
                 issue_id: int or str,
                 parent_id: str,
                 manifest_name: str,
                 repo_name: str,
                 svcresult: dict,
                 is_debug: bool = False):
        """
        Created:
            3-Dec-2019
            craig.trim@ibm.com
            *   https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1509#issuecomment-16376091
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

        fields = []

        def created_at():
            return self._svcresult["created_at"].split('T')[0].strip()

        def open_by():
            if 'actor' in self._svcresult:
                return self._svcresult["actor"]["login"].strip()

            if 'user' in self._svcresult:
                return self._svcresult["user"]["login"].strip()

        def assignee():
            if "assignee" in self._svcresult:
                if self._svcresult["assignee"] is not None:
                    return self._svcresult["assignee"]["login"]

            if "requested_reviewer" in self._svcresult:
                if self._svcresult["requested_reviewer"] is not None:
                    return self._svcresult["requested_reviewer"]["login"]

        def title() -> str:
            return str(self._svcresult["title"])

        def state() -> str:
            return str(self._svcresult["state"])

        def additions() -> str:
            return str(self._svcresult["additions"])

        def deletions() -> str:
            return str(self._svcresult["deletions"])

        def closed_by():
            if self._svcresult['closed_by']:
                return self._svcresult["closed_by"]["login"]

        def updated_at():
            if self._svcresult['updated_at']:
                return self._svcresult["updated_at"].split('T')[0].strip()

        # dates
        fields.append(self._field(field_type="date",
                                  field_name="OpenDate",
                                  field_value=created_at()))

        fields.append(self._field(field_type="date",
                                  field_name="UpdateDate",
                                  field_value=updated_at()))

        # users
        fields.append(self._field(field_type="user",
                                  field_name="OpenBy",
                                  field_value=open_by()))

        fields.append(self._field(field_type="user",
                                  field_name="Assignee",
                                  field_value=assignee()))

        fields.append(self._field(field_type="user",
                                  field_name="CloseBy",
                                  field_value=closed_by()))

        # text
        fields.append(self._field(field_type="long-text",
                                  field_name="Title",
                                  field_value=title()))

        fields.append(self._field(field_type="text",
                                  field_name="State",
                                  field_value=state()))

        if "additions" in self._svcresult:
            fields.append(self._field(field_type="text",
                                      field_name="Additions",
                                      field_value=additions()))

        if "deletions" in self._svcresult:
            fields.append(self._field(field_type="text",
                                      field_name="Deletions",
                                      field_value=deletions()))

        # URLs
        if "commit_url" in self._svcresult:
            fields.append(self._field(field_type="url",
                                      field_name="CommitURL",
                                      field_value=self._svcresult["commit_url"]))

        if "comments_url" in self._svcresult:
            fields.append(self._field(field_type="url",
                                      field_name="CommentsURL",
                                      field_value=self._svcresult["comments_url"]))

        if "events_url" in self._svcresult:
            fields.append(self._field(field_type="url",
                                      field_name="EventsURL",
                                      field_value=self._svcresult["events_url"]))

        records.append(RecordStructure.github_record(fields=fields,
                                                     div_field="pull-request-issue",
                                                     issue_id=self._issue_id,
                                                     key_field=self._record_id(),
                                                     key_field_parent=self._parent_id,
                                                     repo_name=self._repo_name,
                                                     manifest_name=self._manifest_name))

        records = [x for x in records if x]
        if self._is_debug:
            self.logger.debug('\n'.join([
                "Pull Request Tranformation Complete",
                f"\tIssue #{self._issue_id}",
                f"\tTotal Records: {len(records)}"]))

        return records
