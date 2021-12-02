#!/usr/bin/env python
# -*- coding: UTF-8 -*-


import pprint
from typing import Optional

from base import BaseObject
from base import MandatoryParamError


class TransformGitHubStructure(BaseObject):
    """ Transform a GitHub Object to a Cendant Record Structure """

    __keystructs_issue = [
        'assignee,assignees,author_association,body,closed_at,closed_by,comments,comments_url,created_at,events_url,html_url,id,labels,labels_url,locked,milestone,node_id,number,repository_url,state,title,updated_at,url,user']

    __keystructs_comment = [
        'author_association,body,created_at,html_url,id,issue_url,node_id,updated_at,url,user']

    __keystructs_commit_comment = [
        "author_association,body,commit_id,created_at,html_url,id,line,node_id,path,position,updated_at,url,user"]

    __keystructs_pr_event = [
        "actor,commit_id,commit_url,created_at,event,id,node_id,requested_reviewer,review_requester,url",
        "actor,assignee,assigner,commit_id,commit_url,created_at,event,id,node_id,url",
        "actor,commit_id,commit_url,created_at,event,id,label,node_id,url"]

    __keystructs_merge = [
        "author,committer,html_url,message,node_id,parents,sha,tree,url,verification"]

    __keystructs_pr_issue = [
        "_links,additions,assignee,assignees,author_association,base,body,changed_files,closed_at,comments,comments_url,commits,commits_url,created_at,deletions,diff_url,head,html_url,id,issue_url,labels,locked,maintainer_can_modify,merge_commit_sha,mergeable,mergeable_state,merged,merged_at,merged_by,milestone,node_id,number,patch_url,rebaseable,requested_reviewers,requested_teams,review_comment_url,review_comments,review_comments_url,state,statuses_url,title,updated_at,url,user",
        "assignee,assignees,author_association,body,closed_at,closed_by,comments,comments_url,created_at,events_url,html_url,id,labels,labels_url,locked,milestone,node_id,number,pull_request,repository_url,state,title,updated_at,url,user"]

    __keystructs_commits = [
        "author,comments_url,commit,committer,files,html_url,node_id,parents,sha,stats,url"]

    __keystructs_mentions = [
        "actor,commit_id,commit_url,created_at,event,id,node_id,url"]

    __keystructs_milestone = [
        "actor,commit_id,commit_url,created_at,event,id,milestone,node_id,url"]

    __keystructs_renamed = [
        "actor,commit_id,commit_url,created_at,event,id,node_id,rename,url"]

    __keystructs_review_dismissed = [
        "actor,commit_id,commit_url,created_at,dismissed_review,event,id,node_id,url"]

    __keystructs_tree = [
        "sha,tree,truncated,url"]

    def __init__(self,
                 issue_id: int or str,
                 parent_id: str,
                 manifest_name: str,
                 repo_name: str,
                 repo_owner: str,
                 svcresult: dict or list,
                 is_debug: bool = False,
                 log_structures: bool = True):
        """
        Created:
            26-Nov-2019
            craig.trim@ibm.com
            *   https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1459
        Updated:
            3-Dec-2019
            craig.trim@ibm.com
            *   add further differentiation based on structural attributes
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1509#issue-11034406
        Updated:
            7-Dec-2019
            craig.trim@ibm.com
            *   renamed from 'transform-github-data'
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1553#issue-11089485
            *   integrate zenhub access
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1511
        Updated:
            26-Dec-2019
            craig.trim@ibm.com
            *   update zenhub access pattern for pics
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1667
        :param issue_id:
            this is a provenance link to the source issue
            in this case it would be the original GitHub Issue ID the comments are linked to
        :param repo_name:
            the name of the repository the data is drawn from
        :param manifest_name:
            this is a provenance link to the source data
            in this case it would be the original GitHub URL the data was taken from
        :param svcresult:
            a list of data retrieved from GitHub
        :param is_debug:
        :param log_structures:
            write JSON structures to log file
            these objects are potentially LARGE - best to keep this value False for production runs
        """
        BaseObject.__init__(self, __name__)
        from datagit.ingest.github.svc import AccessZenHubURL

        if not parent_id:
            raise MandatoryParamError

        self._issue_id = issue_id
        self._parent_id = parent_id
        self._is_debug = is_debug
        self._svcresult = svcresult
        self._repo_name = repo_name
        self._repo_owner = repo_owner
        self._manifest_name = manifest_name
        self._log_structures = log_structures

        self._zenhub_access = AccessZenHubURL(is_debug=self._is_debug,
                                              repo_name=self._repo_name,
                                              repo_owner=self._repo_owner)

    def _log_result(self,
                    structure_name: str,
                    svcresult: dict or list) -> None:
        if self._log_structures:
            self.logger.debug('\n'.join([
                f"{structure_name} Structure Detected",
                "------------------------------------------------------------------------------------------",
                ','.join(sorted(svcresult.keys())),
                pprint.pformat(svcresult),
                "------------------------------------------------------------------------------------------"]))

    def _analyze_structure(self,
                           svcresult: dict) -> list or None:
        from datagit.ingest.github.dmo import IssueTransformation
        from datagit.ingest.github.dmo import AssignmentTransformation
        from datagit.ingest.github.dmo import CommentsTransformation
        from datagit.ingest.github.dmo import CommitsTransformation
        from datagit.ingest.github.dmo import PullRequestEventTransformation
        from datagit.ingest.github.dmo import PullRequestIssueTransformation
        from datagit.ingest.github.dmo import MentionsTransformation
        from datagit.ingest.github.dmo import CommitCommentTransformation
        from datagit.ingest.github.dmo import MergeEventTransformation
        key_struct = ','.join(sorted(svcresult.keys()))

        def issue_transformation() -> list:
            self._log_result("Issues", svcresult)

            d_zenhub_result = self._zenhub_access.process(issue_id=self._issue_id)

            def _zenhub_epic() -> Optional[dict]:
                if d_zenhub_result['is_epic']:
                    return self._zenhub_access.process(issue_id=self._issue_id,
                                                       as_epic=True)

            return IssueTransformation(svcresult=svcresult,
                                       is_debug=self._is_debug,
                                       issue_id=self._issue_id,
                                       parent_id=self._parent_id,
                                       repo_name=self._repo_name,
                                       zenhub_result=d_zenhub_result,
                                       zenhub_epic=_zenhub_epic(),
                                       manifest_name=self._manifest_name).process()

        def comments_transformation() -> list:
            self._log_result("Comments", svcresult)
            return CommentsTransformation(svcresult=svcresult,
                                          is_debug=self._is_debug,
                                          issue_id=self._issue_id,
                                          parent_id=self._parent_id,
                                          repo_name=self._repo_name,
                                          manifest_name=self._manifest_name).process()

        def pull_request_issues() -> list:
            self._log_result("Pull Request Issue", svcresult)
            return PullRequestIssueTransformation(svcresult=svcresult,
                                                  is_debug=self._is_debug,
                                                  issue_id=self._issue_id,
                                                  parent_id=self._parent_id,
                                                  repo_name=self._repo_name,
                                                  manifest_name=self._manifest_name).process()

        def merge_request_event() -> list:
            self._log_result("Merge Request Event", svcresult)
            return MergeEventTransformation(svcresult=svcresult,
                                            is_debug=self._is_debug,
                                            issue_id=self._issue_id,
                                            parent_id=self._parent_id,
                                            repo_name=self._repo_name,
                                            manifest_name=self._manifest_name).process()

        def pull_request_events() -> list:
            self._log_result("Pull Request Event", svcresult)
            return PullRequestEventTransformation(svcresult=svcresult,
                                                  is_debug=self._is_debug,
                                                  issue_id=self._issue_id,
                                                  parent_id=self._parent_id,
                                                  repo_name=self._repo_name,
                                                  manifest_name=self._manifest_name).process()

        def commit_comment_transformation() -> list:
            self._log_result("Commit Comment", svcresult)
            return CommitCommentTransformation(svcresult=svcresult,
                                               is_debug=self._is_debug,
                                               issue_id=self._issue_id,
                                               parent_id=self._parent_id,
                                               repo_name=self._repo_name,
                                               manifest_name=self._manifest_name).process()

        def assignment_transformation() -> list:
            self._log_result("Assignment", svcresult)
            return AssignmentTransformation(svcresult=svcresult,
                                            is_debug=self._is_debug,
                                            issue_id=self._issue_id,
                                            parent_id=self._parent_id,
                                            repo_name=self._repo_name,
                                            manifest_name=self._manifest_name).process()

        def commits_transformation() -> list:
            self._log_result("Commit", svcresult)
            return CommitsTransformation(svcresult=svcresult,
                                         is_debug=self._is_debug,
                                         issue_id=self._issue_id,
                                         parent_id=self._parent_id,
                                         repo_name=self._repo_name,
                                         manifest_name=self._manifest_name).process()

        def mentions_transformation() -> list:
            self._log_result("Mentions", svcresult)
            return MentionsTransformation(svcresult=svcresult,
                                          is_debug=self._is_debug,
                                          issue_id=self._issue_id,
                                          parent_id=self._parent_id,
                                          repo_name=self._repo_name,
                                          manifest_name=self._manifest_name).process()

        def unrecognized() -> None:
            self.logger.warning('\n'.join([
                "Unrecognized GitHub Object",
                f"\tIssue #{self._issue_id}",
                f"\tKeys: {key_struct}",
                "--------------------------------------------------",
                pprint.pformat(svcresult, indent=4),
                "--------------------------------------------------"]))
            raise NotImplementedError

        # Controller Logic
        if key_struct in self.__keystructs_milestone:  # GIT-1509-16376091
            pass  # not interested
        elif key_struct in self.__keystructs_renamed:  # GIT-1509-16376485
            pass  # not interested
        elif key_struct in self.__keystructs_review_dismissed:  # GIT-1509-16376649
            pass  # not interested
        elif key_struct in self.__keystructs_issue:
            return issue_transformation()
        elif key_struct in self.__keystructs_merge:
            return merge_request_event()
        elif key_struct in self.__keystructs_comment:
            return comments_transformation()
        elif key_struct in self.__keystructs_commit_comment:
            return commit_comment_transformation()
        elif key_struct in self.__keystructs_pr_issue:
            return pull_request_issues()
        elif key_struct in self.__keystructs_pr_event:
            if 'event' not in svcresult:
                return pull_request_events()
            elif svcresult['event'] == 'assigned':  # GIT-1509-11034406
                return assignment_transformation()
            elif svcresult['event'] == 'referenced':
                return pull_request_events()
        elif key_struct in self.__keystructs_commits:
            return commits_transformation()
        elif key_struct in self.__keystructs_mentions:
            if svcresult["event"] == "pinned" or \
                    svcresult["event"] == "unpinned":  # GIT-1509-16375937
                pass  # not interested
            elif svcresult["event"] == "locked" or \
                    svcresult["event"] == "unlocked":  # GIT-1509-16375958
                pass  # not interested
            elif svcresult["event"] == "merged":  # GIT-1509-16376390
                pass  # not interested
            elif svcresult["event"] == "closed":  # GIT-1509-16376401
                pass  # not interested
            else:
                return mentions_transformation()
        elif key_struct in self.__keystructs_tree:  # GIT-1537-16466899
            pass  # not interested
        else:
            unrecognized()

    def _analyze(self,
                 svcresult: list or dict) -> list or None:
        """
        Purpose:
            Analyze the service result (svcresult) from GitHub
        :param svcresult:
            the incoming svcresult may be either:
            list        multiple structures
            dict        a single structure
        :return:
            a list of records
        """

        if type(svcresult) == list:
            master_records = []
            for inner_structure in list(svcresult):

                if not inner_structure:
                    raise ValueError

                results = self._analyze_structure(inner_structure)
                if results and len(results):
                    master_records += results

            return master_records

        elif type(svcresult) == dict:
            return self._analyze_structure(dict(svcresult))

        raise NotImplementedError

    def process(self) -> list:
        return self._analyze(self._svcresult)
