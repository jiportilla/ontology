#!/usr/bin/env python
# -*- coding: UTF-8 -*-


import pprint
from typing import Optional

from base import BaseObject
from base import DataTypeError
from base import FieldStructure
from base import MandatoryParamError
from base import RecordStructure


class IssueTransformation(BaseObject):
    """ Transform GitHub Issue into the Cendant Field Structure """

    def __init__(self,
                 issue_id: int or str,
                 parent_id: str,
                 manifest_name: str,
                 repo_name: str,
                 svcresult: dict,
                 zenhub_result: dict,
                 zenhub_epic: Optional[dict],
                 is_debug: bool = False):
        """
        Created:
            29-Nov-2019
            craig.trim@ibm.com
            *   https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1478
        Updated:
            7-Dec-2019
            craig.trim@ibm.com
            *   integrate zenhub access
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1511
        Updated:
            18-Dec-2019
            craig.trim@ibm.com
            *   put check in place for comments-url
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1626#issuecomment-16729867
        Updated:
            26-Dec-2019
            craig.trim@ibm.com
            *   add zenhub-epic param and 'epic-assignment' field set
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1667#issuecomment-16821516
        :param issue_id:
            this is a provenance link to the source issue
            in this case it would be the original GitHub Issue ID the comments are linked to
        :param repo_name:
            the name of the repository the data is drawn from
        :param manifest_name:
            this is a provenance link to the source data
            in this case it would be the original GitHub URL the data was taken from
        :param svcresult:
            an event from GitHub
        :param zenhub_result:
            a basic ZenHub result
            Sample Input:
                {   'plus_ones': [],
                    'is_epic': True }
        :param zenhub_epic:
            a ZenHub epic dictionary
            Reference:
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1667#issuecomment-16821516
            Sample Input:
                {   'issues': [
                        {   'estimate': {'value': 8},
                            'is_epic': False,
                            'issue_number': 786,
                            'pipeline': {'name': 'closed'},
                            'pipelines': [{}],
                            'repo_id': 508363 },
                            ...
                        {   'estimate': {'value': 8},
                            'is_epic': False,
                            'issue_number': 916,
                            'pipeline': {'name': 'closed'},
                            'pipelines': [{}],
                            'repo_id': 508363 }],
                    'total_epic_estimates': { 'value': 51 }}
        :param is_debug:
        """
        BaseObject.__init__(self, __name__)

        if type(svcresult) != dict:
            raise DataTypeError("Service Result, dict")

        if not issue_id:
            raise MandatoryParamError("Issue ID")

        self._is_debug = is_debug
        self._issue_id = issue_id
        self._parent_id = parent_id
        self._svcresult = svcresult
        self._repo_name = repo_name
        self._zenhub_epic = zenhub_epic
        self._zenhub_result = zenhub_result
        self._manifest_name = manifest_name

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

        def title():
            return self._svcresult["title"].strip()

        def body():
            return self._svcresult["body"].strip()

        def labels() -> str:
            """
            Purpose:
                Transform the label structure into a simple CSV string
            Sample Input:
                [   {   'id': 4198015, 'node_id': 'MDU6TGFiZWw0MTk4MDE1',
                        'url': 'https://github.ibm.com/api/v3/repos/GTS-CDO/unstructured-analytics/labels/enhancement',
                        'name': 'enhancement',
                        'color': '1d76db',
                        'default': True }]
            Sample Output:
                'enhancement'
            :return:
                a comma-delimited string of labels
            """
            return ', '.join(sorted([label['name'] for label in self._svcresult["labels"]]))

        def state():
            return self._svcresult["state"].strip()

        def closed_at():
            if self._svcresult['closed_at']:
                return self._svcresult["closed_at"].split('T')[0].strip()

        def closed_by():
            if self._svcresult['closed_by']:
                return self._svcresult["closed_by"]["login"]

        def created_at():
            return self._svcresult["created_at"].split('T')[0].strip()

        def updated_at():
            if self._svcresult['updated_at']:
                return self._svcresult["updated_at"].split('T')[0].strip()

        def open_by():
            return self._svcresult["user"]["login"].strip()

        def assignees() -> str:
            logins = set()
            for d_assignee in self._svcresult["assignees"]:
                logins.add(d_assignee["login"])
            return ', '.join(sorted(logins))

        def comments_url() -> str or None:
            if self._svcresult["comments"] > 0:  # GIT-1626-16729867
                return self._svcresult["comments_url"]

        if self._zenhub_result:  # GIT-1554

            if self._is_debug:
                self.logger.debug('\n'.join([
                    "ZenHub Result Integration",
                    pprint.pformat(self._zenhub_result)]))

            if 'is_epic' in self._zenhub_result:
                fields.append(self._field(field_type="text",
                                          field_name="IsEpic",
                                          field_value=self._zenhub_result['is_epic']))

            if 'pipeline' in self._zenhub_result:
                fields.append(self._field(field_type="text",
                                          field_name="Pipeline",
                                          field_value=self._zenhub_result['pipeline']['name']))

            if 'estimate' in self._zenhub_result:
                fields.append(self._field(field_type="text",
                                          field_name="Estimate",
                                          field_value=self._zenhub_result['estimate']['value']))

        fields.append(self._field(field_type="text",
                                  field_name="ID",
                                  field_value=self._svcresult["id"]))

        fields.append(self._field(field_type="long-text",
                                  field_name="Body",
                                  field_value=body()))

        fields.append(self._field(field_type="long-text",
                                  field_name="Title",
                                  field_value=title()))

        fields.append(self._field(field_type="text",
                                  field_name="Labels",
                                  field_value=labels()))

        fields.append(self._field(field_type="text",
                                  field_name="State",
                                  field_value=state()))

        fields.append(self._field(field_type="date",
                                  field_name="OpenDate",
                                  field_value=created_at()))

        fields.append(self._field(field_type="date",
                                  field_name="UpdateDate",
                                  field_value=updated_at()))

        fields.append(self._field(field_type="date",
                                  field_name="CloseDate",
                                  field_value=closed_at()))

        fields.append(self._field(field_type="user",
                                  field_name="OpenBy",
                                  field_value=open_by()))

        fields.append(self._field(field_type="user",
                                  field_name="CloseBy",
                                  field_value=closed_by()))

        fields.append(self._field(field_type="user",
                                  field_name="Assignees",
                                  field_value=assignees()))

        fields.append(self._field(field_type="url",
                                  field_name="EventsURL",
                                  field_value=self._svcresult["events_url"]))

        if self._zenhub_epic:
            for issue in self._zenhub_epic['issues']:
                fields.append(self._field(field_type="ID",
                                          field_name="EpicAssignment",
                                          field_value=issue["issue_number"]))

        if self._svcresult["comments"] > 0:
            fields.append(self._field(field_type="url",
                                      field_name="CommentsURL",
                                      field_value=comments_url()))

        if "pull_request" in self._svcresult:
            fields.append(self._field(field_type="url",
                                      field_name="PullRequestURL",
                                      field_value=self._svcresult["pull_request"]["url"]))

        records.append(RecordStructure.github_record(fields=fields,
                                                     div_field="issue",
                                                     issue_id=self._issue_id,
                                                     key_field=self._issue_id,
                                                     key_field_parent=self._parent_id,
                                                     repo_name=self._repo_name,
                                                     manifest_name=self._manifest_name))

        records = [x for x in records if x]
        if self._is_debug:
            self.logger.debug('\n'.join([
                "Issue Tranformation Complete",
                f"\tIssue #{self._issue_id}",
                f"\tTotal Records: {len(records)}"]))

        return records
