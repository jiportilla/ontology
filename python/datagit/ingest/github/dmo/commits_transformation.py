#!/usr/bin/env python
# -*- coding: UTF-8 -*-


import pprint
from typing import Optional

from base import BaseObject
from base import DataTypeError
from base import FieldStructure
from base import MandatoryParamError
from base import RecordStructure


class CommitsTransformation(BaseObject):
    """ Transform GitHub Commit into the Cendant Field Structure """

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
            *   https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1484
        Updated:
            18-Dec-2019
            craig.trim@ibm.com
            *   update access for 'author' attribute
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1626#issuecomment-16730068
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

        def author() -> Optional[str]:
            if event["author"]:  # GIT-1626-16730068
                return event["author"]["login"].strip()

            if event["commit"]:
                return event["commit"]["author"]["name"].strip()

            self.logger.warning('\n'.join([
                "Commit Pattern Lacks Authorship",
                f"\tIssue ID: {self._issue_id}",
                f"\tParent ID: {self._parent_id}"]))

        def additions() -> str:
            return str(event["stats"]["additions"])

        def deletions() -> str:
            return str(event["stats"]["deletions"])

        def total() -> str:
            return str(event["stats"]["total"])

        def title() -> str:
            return str(event["commit"]["message"])

        def commit_date() -> str:
            if "author" in event["commit"]:
                return event["commit"]["author"]["date"].split('T')[0].strip()

            if "commiter" in event["commit"]:
                return event["commit"]["committer"]["date"].split('T')[0].strip()

            self.logger.warning('\n'.join([
                "Commit Date Not Found",
                pprint.pformat(event, indent=4)]))
            raise NotImplementedError

        fields.append(self._field(field_type="text",
                                  field_name="Additions",
                                  field_value=additions()))

        fields.append(self._field(field_type="text",
                                  field_name="Deletions",
                                  field_value=deletions()))

        fields.append(self._field(field_type="text",
                                  field_name="Total",
                                  field_value=total()))

        fields.append(self._field(field_type="text",
                                  field_name="OpenBy",
                                  field_value=author()))

        fields.append(self._field(field_type="text",
                                  field_name="OpenDate",
                                  field_value=commit_date()))

        fields.append(self._field(field_type="long-text",
                                  field_name="Title",
                                  field_value=title()))

        fields.append(self._field(field_type="url",
                                  field_name="CommitURL",
                                  field_value=event["commit"]["url"]))

        fields.append(self._field(field_type="url",
                                  field_name="CommentsURL",
                                  field_value=event["comments_url"]))

        return RecordStructure.github_record(fields=fields,
                                             div_field="commit",
                                             key_field=self._record_id(),
                                             key_field_parent=self._parent_id,
                                             issue_id=self._issue_id,
                                             repo_name=self._repo_name,
                                             manifest_name=self._manifest_name)

    def process(self) -> list:

        records = [self._commit_record(self._svcresult)]

        ctr = 1
        commit_id = self._record_id()

        for file in self._svcresult["files"]:
            fields = [
                self._field(field_type="text",
                            field_name="file",
                            field_value=file["filename"]),

                self._field(field_type="text",
                            field_name="status",
                            field_value=file["status"]),

                self._field(field_type="text",
                            field_name="changes",
                            field_value=file["changes"]),

                self._field(field_type="url",
                            field_name="RawURL",
                            field_value=file["raw_url"])]

            records.append(RecordStructure.github_record(fields=fields,
                                                         div_field="file-commit",
                                                         issue_id=self._issue_id,
                                                         key_field=f"{commit_id}-{ctr}",
                                                         key_field_parent=self._parent_id,
                                                         repo_name=self._repo_name,
                                                         manifest_name=self._manifest_name))

            ctr += 1

        records = [x for x in records if x]
        if self._is_debug:
            self.logger.debug('\n'.join([
                "Commit Tranformation Complete",
                f"\tIssue #{self._issue_id}",
                f"\tTotal Records: {len(records)}"]))

        return records
