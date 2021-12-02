# !/usr/bin/env python
# -*- coding: UTF-8 -*-


from pandas import DataFrame

from base import BaseObject
from datagit.graph.dmo import AssigneeNodeBuilder
from datagit.graph.dmo import AssignerNodeBuilder
from datagit.graph.dmo import CommentNodeBuilder
from datagit.graph.dmo import GraphEdgeDefGenerator
from datagit.graph.dmo import IssueNodeBuilder
from datagit.graph.svc import GitHubRecordFinder


class GraphSocialNetwork(BaseObject):
    """
    Purpose:

    Traceability:

    Prereq:
    """

    __stylesheet_path = "resources/config/graph/graphviz_github_social.yml"

    def __init__(self,
                 pattern: dict,
                 d_index: dict,
                 df_social_entity_analysis: DataFrame,
                 df_social_relationship_analysis: DataFrame,
                 is_debug: bool = True):
        """
        Created:
            30-Dec-2019
            craig.trim@ibm.com
            *   refactored out of 'graph-github-issue'
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1678
        Updated:
            31-Dec-2019
            craig.trim@ibm.com
            *   pass stylesheet path as a parameter
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1684
            *   use record finder service
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1681#issuecomment-16877247
        Updated:
            2-Jan-2020
            craig.trim@ibm.com
            *   pass social dataframes as runtime parameters
                https://github.ibm.com/-cdo/unstructured-analytics/issues/1680
        :param is_debug:
            if True     increase log output at DEBUG level
        """
        BaseObject.__init__(self, __name__)
        self._is_debug = is_debug
        self._df_social_entity_analysis = df_social_entity_analysis
        self._df_social_relationship_analysis = df_social_relationship_analysis

        self._lines = []
        self._triples = []
        self._finder = GitHubRecordFinder(is_debug=self._is_debug,
                                          pattern=pattern,
                                          d_index=d_index)
        self._process()

    def lines(self) -> list:
        return self._lines

    def _add_lines(self,
                   some_lines: list) -> None:
        [self._lines.append(f"\t{line}") for line in some_lines]

    def _add_assignment(self,
                        openby_nodeid):

        for assignment in self._finder.find().assignments():
            assignee_nb = AssigneeNodeBuilder(d_record=assignment,
                                              is_debug=self._is_debug,
                                              stylesheet_path=self.__stylesheet_path,
                                              df_social_entity_analysis=self._df_social_entity_analysis)

            self._add_lines(assignee_nb.state["assignee"]["lines"])
            self._triples.append({
                'subject': assignee_nb.state["assignee"]["node_id"],
                'predicate': 'contributedTo',
                'object': openby_nodeid})

            assigner_nb = AssignerNodeBuilder(d_record=assignment,
                                              is_debug=self._is_debug,
                                              stylesheet_path=self.__stylesheet_path,
                                              df_social_entity_analysis=self._df_social_entity_analysis)

            self._add_lines(assigner_nb.state["assigner"]["lines"])
            self._triples.append({
                'subject': assigner_nb.state["assigner"]["node_id"],
                'predicate': 'assigns',
                'object': openby_nodeid})

    def _add_comments(self,
                      issue_nb: IssueNodeBuilder):

        for comment in self._finder.find().comments():
            comment_nb = CommentNodeBuilder(d_record=comment,
                                            is_debug=self._is_debug,
                                            stylesheet_path=self.__stylesheet_path,
                                            df_social_entity_analysis=self._df_social_entity_analysis)

            self._add_lines(comment_nb.state["OpenBy"]["lines"])
            self._triples.append({
                "subject": comment_nb.state["OpenBy"]["node_id"],
                "subject_name": comment_nb.state["OpenBy"]["person_name"],
                "predicate": "collaborates",
                "object": issue_nb.state["OpenBy"]["node_id"],
                "object_name": issue_nb.state["OpenBy"]["person_name"]})

            for mentions in comment_nb.state["mentions"]:
                self._add_lines(mentions["lines"])

                self._triples.append({
                    "subject": comment_nb.state["OpenBy"]["node_id"],
                    "subject_name": comment_nb.state["OpenBy"]["person_name"],
                    "predicate": "mentions",
                    "object": mentions["node_id"],
                    "object_name": mentions["person_name"]})

    def _process(self) -> None:

        issue_nb = IssueNodeBuilder(is_debug=self._is_debug,
                                    d_record=self._finder.find().issue(),
                                    stylesheet_path=self.__stylesheet_path,
                                    df_social_entity_analysis=self._df_social_entity_analysis)

        openby_nodeid = issue_nb.state["OpenBy"]["node_id"]

        has_assignees = "Assignees" in issue_nb.state and issue_nb.state["Assignees"] is not None
        has_close_by = "CloseBy" in issue_nb.state and issue_nb.state["CloseBy"] is not None

        if has_assignees:
            assignee_nodeid = issue_nb.state["Assignees"]["node_id"]
            if openby_nodeid != assignee_nodeid:
                self._triples.append({
                    'subject': openby_nodeid,
                    'predicate': 'assignedTo',
                    'object': assignee_nodeid})

        if has_close_by:
            closeby_nodeid = issue_nb.state["CloseBy"]["node_id"]
            if openby_nodeid != closeby_nodeid:
                self._triples.append({
                    'subject': openby_nodeid,
                    'predicate': 'collaborates',
                    'object': closeby_nodeid})

            if has_assignees:
                assignee_nodeid = issue_nb.state["Assignees"]["node_id"]
                if assignee_nodeid != closeby_nodeid:
                    self._triples.append({
                        'subject': openby_nodeid,
                        'predicate': 'collaborates',
                        'object': assignee_nodeid})

        if self._finder.find().has_assignment():
            self._add_assignment(openby_nodeid)

        if self._finder.find().has_comments():
            self._add_comments(issue_nb)

        self._lines += GraphEdgeDefGenerator(triples=self._triples,
                                             is_debug=self._is_debug,
                                             stylesheet_path=self.__stylesheet_path,
                                             df_social_rel_analysis=self._df_social_relationship_analysis).process()
