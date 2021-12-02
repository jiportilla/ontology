# !/usr/bin/env python
# -*- coding: UTF-8 -*-


from base import BaseObject
from datagit.graph.dmo import AssigneeNodeBuilder
from datagit.graph.dmo import AssignerNodeBuilder
from datagit.graph.dmo import CommentNodeBuilder
from datagit.graph.dmo import CommitNodeBuilder
from datagit.graph.dmo import EpicAssignmentNodeBuilder
from datagit.graph.dmo import FileCommitNodeBuilder
from datagit.graph.dmo import FileFolderNodeBuilder
from datagit.graph.dmo import FileImportNodeBuilder
from datagit.graph.dmo import FileNameNodeBuilder
from datagit.graph.dmo import GraphEdgeDefGenerator
from datagit.graph.dmo import IssueNodeBuilder
from datagit.graph.dmo import IssueReferenceNodeId
from datagit.graph.dmo import PullRequestNodeBuilder
from datagit.graph.svc import GitHubRecordFinder


class GraphGitHubIssue(BaseObject):
    """
    Purpose:

    Traceability:

    Prereq:
    """

    __stylesheet_path = "resources/config/graph/graphviz_github_issue.yml"

    def __init__(self,
                 pattern: dict,
                 d_index: dict,
                 social_only: bool = True,
                 is_debug: bool = True):
        """
        Created:
            17-Dec-2019
            craig.trim@ibm.com
            *   https://github.ibm.com/-cdo/unstructured-analytics/issues/1542
        Updated:
            24-Dec-2019
            craig.trim@ibm.com
            *   add edge styling
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1645
        Updated:
            26-Dec-2019
            craig.trim@ibm.com
            *   refactored in pursuit of
                https://github.ibm.com/-cdo/unstructured-analytics/issues/1646
            *   modify file commit edges
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1648#issuecomment-16817744
            *   add epic assignments
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1667#issuecomment-16821672
        :param is_debug:
            if True     increase log output at DEBUG level
        """
        BaseObject.__init__(self, __name__)

        self._lines = []
        self._triples = []
        # self._index = d_index
        # self._pattern = pattern
        self._is_debug = is_debug
        self._social_only = social_only

        self._finder = GitHubRecordFinder(is_debug=self._is_debug,
                                          pattern=pattern,
                                          d_index=d_index)

        # api = PythonParseAPI(is_debug=self._is_debug)
        # if self.__df_src is None:
        #     self.__df_src = api.load(date="20191224").source()

        self._process()

    def lines(self) -> list:
        return self._lines

    def _add_lines(self,
                   some_lines: list) -> None:
        [self._lines.append(f"\t{line}") for line in some_lines]

    def _add_assignment(self,
                        issue_nb: IssueNodeBuilder):

        for assignment in self._record().assignments():
            assignee_nb = AssigneeNodeBuilder(is_debug=self._is_debug,
                                              d_record=assignment)
            self._add_lines(assignee_nb.lines)
            self._triples.append({
                'subject': assignee_nb.node_id,
                'predicate': 'assignedTo',
                'object': issue_nb.node_id})

            assigner_nb = AssignerNodeBuilder(is_debug=self._is_debug,
                                              d_record=assignment)
            self._add_lines(assigner_nb.lines)
            self._triples.append({
                'subject': assigner_nb.node_id,
                'predicate': 'assigns',
                'object': assignee_nb.node_id})

    def _add_comments(self,
                      issue_nb: IssueNodeBuilder):

        for comment in self._record().comments():
            comment_nb = CommentNodeBuilder(is_debug=self._is_debug,
                                            d_record=comment)
            self._add_lines(comment_nb.lines)
            self._triples.append({
                'subject': comment_nb.node_id,
                'predicate': 'contributedTo',
                'object': issue_nb.node_id})
            self._triples += comment_nb.triples

            if not self._social_only:
                issue_ref_ids = IssueReferenceNodeId(is_debug=self._is_debug,
                                                     d_record=comment).node_ids
                for issue_ref_id in issue_ref_ids:
                    self._triples.append({
                        'subject': issue_nb.node_id,
                        'predicate': 'references',
                        'object': issue_ref_id})
                    self._triples.append({
                        'subject': comment_nb.node_id,
                        'predicate': 'references',
                        'object': issue_ref_id})

    def _add_file_commit(self,
                         file: dict,
                         commit_nb: CommitNodeBuilder,
                         fileimport_nb: FileImportNodeBuilder):
        filecommit_nb = FileCommitNodeBuilder(is_debug=self._is_debug,
                                              d_record=file)
        df2 = self._record().file_records(filecommit_nb)
        if df2.empty:
            return None

        filename_nb = FileNameNodeBuilder(df=df2,
                                          is_debug=self._is_debug)
        self._add_lines(filename_nb.lines)
        self._triples.append({
            'subject': commit_nb.node_id,
            'predicate': 'modifies',
            'object': filename_nb.node_id,
            'label': f"Δ{filecommit_nb.changes}",
            'fontsize': filecommit_nb.fontsize})

        # look up each PR File with the corresponding Python File
        for d_fileimport in fileimport_nb.process(df=df2):
            self._add_lines(d_fileimport["lines"])
            self._triples.append({
                'subject': filename_nb.node_id,
                'predicate': 'imports',
                'object': d_fileimport["node_id"]})

            filefolder_nb = FileFolderNodeBuilder(df=df2,
                                                  is_debug=self._is_debug)

            [self._add_lines(lines) for lines in filefolder_nb.lines]
            [self._triples.append(triple) for triple in filefolder_nb.triples]

            filefolder_obj = filefolder_nb.triples[len(filefolder_nb.triples) - 1]['object']
            self._triples.append({
                'subject': filefolder_obj,
                'predicate': 'composes',
                'object': filename_nb.node_id})

    def _add_pull_requests(self,
                           issue_nb: IssueNodeBuilder,
                           fileimport_nb: FileImportNodeBuilder):
        # A Pull Request
        for pull_request in self._record().pull_requests():
            pullrequest_nb = PullRequestNodeBuilder(is_debug=self._is_debug,
                                                    d_record=pull_request)

            commit_nb = CommitNodeBuilder(is_debug=self._is_debug,
                                          d_record=self._record().commit(pull_request))
            if commit_nb.is_merge_commit:
                continue

            self._add_lines(commit_nb.lines)

            self._triples.append({
                'subject': issue_nb.node_id,
                'predicate': 'commits',
                'object': commit_nb.node_id})

            # has multiple files
            for file in self._record().files(pull_request):
                self._add_file_commit(file=file,
                                      commit_nb=commit_nb,
                                      fileimport_nb=fileimport_nb)

    def _process(self) -> None:

        fileimport_nb = FileImportNodeBuilder(is_debug=False,
                                              collection_date="20191224")

        issue_nb = IssueNodeBuilder(is_debug=self._is_debug,
                                    d_record=self._record().issue())
        self._add_lines(issue_nb.lines)

        if not self._social_only:
            epicassignment_nb = EpicAssignmentNodeBuilder(is_debug=self._is_debug,
                                                          d_record=self._record().issue())
            for assignment_node_id in epicassignment_nb.node_ids:
                self._triples.append({
                    'subject': assignment_node_id,
                    'predicate': 'partOf',
                    'object': issue_nb.node_id})

        if self._record().has_assignment():
            self._add_assignment(issue_nb)

        if self._record().has_comments():
            self._add_comments(issue_nb)

        if not self._social_only:
            self._add_pull_requests(issue_nb=issue_nb,
                                    fileimport_nb=fileimport_nb)

        self._lines += GraphEdgeDefGenerator(triples=self._triples,
                                             is_debug=self._is_debug).process()
