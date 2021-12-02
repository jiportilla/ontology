# !/usr/bin/env python
# -*- coding: UTF-8 -*-

from base import BaseObject


class FindFlowHierarchy(BaseObject):
    """ Act as a controller in front of all flow hierarchy dicts    """

    def __init__(self, some_input):
        """
        Created:
            14-Mar-2017
            craig.trim@ibm.com
        Updated:
            5-Apr-2017
            craig.trim@ibm.com
            *   use new dictionaries
        Updated:
            28-Jun-2017
            craig.trim@ibm.com
            *   refactor out logic into
                get-parent
                get-children
        Updated:
            22-Jan-2018
            craig.trim@ibm.com
            *   added 'exists' method
                https://gain-jira-prd2.gain.sl.edst.ibm.com:8443/browse/AGP-2255
        Updated:
            8-Aug-2018
            craig.trim@ibm.com
            *   added '_parent_flow' from 'find-recent-dialogs'
                https://gain-jira-prd2.gain.sl.edst.ibm.com:8443/browse/AGP-6598
        Updated:
            17-Jul-2019
            craig.trim@ibm.com
            *   migrated from abacus-att
                git@github.ibm.com:abacus-implementation/abacus-att.git
        :param some_input:
            some flow to test against
        """
        BaseObject.__init__(self, __name__)
        from datadict import the_flow_taxonomy_dict
        from datadict import the_flow_taxonomy_revmap

        if some_input is None:
            raise ValueError

        self.input = self._normalize(some_input)
        self.flow_dict = the_flow_taxonomy_dict
        self.flow_revmap = the_flow_taxonomy_revmap

    @staticmethod
    def _normalize(some_flow_name):
        return some_flow_name.lower().strip()

    def is_answer(self):
        return self.input.startswith("answer_")

    def exists(self):
        if self.input in self.flow_revmap:
            return True
        return self.input in self.flow_dict

    def has_parent(self, some_expected_parent):
        """
        Purpose:
            determine if a given flow matches the expected parent
        :param some_expected_parent:
            the expected parent
        :return:
            True if the match occurs as expected
        """
        some_expected_parent = self._normalize(some_expected_parent)
        if len(some_expected_parent) == 0:
            raise ValueError("Input Parameter is Mandatory")

        some_actual_parent = self.get_parent()
        return some_expected_parent == some_actual_parent

    def get_parent(self):
        """
        Purpose:
            find a taxonomical parent for a given flow
        :return:
            the parent for the flow
                cardinality is 0..*
        """
        if self.input in self.flow_revmap:
            parent = self.flow_revmap[self.input]
            if parent is not None:
                return parent

        if self.input.upper().startswith("GOSS_CONTRACT"):
            return "GOSS_CONTRACT"

        if self.is_answer():
            return "ANSWER"

    def get_children(self):
        """
        Purpose:
            all the child flows of a given flow
        :return:
            the child flows
                cardinality is 0..*
        """
        if self.input in self.flow_dict:
            return self.flow_dict[self.input]

    def is_ancestor_of(self, some_expected_child):
        """
        Purpose:
            determine if a given flow is a parent of the expected child
        :param some_expected_child:
            the expected child
        :return:
            True if the match occurs as expected
        """
        some_expected_child = self._normalize(some_expected_child)
        if 0 == len(some_expected_child):
            raise ValueError("Input Parameter is Mandatory")

        current = some_expected_child
        while current in self.flow_revmap and self.flow_revmap[current] != "goss":
            current = self.flow_revmap[current]
            if current == self.input:
                return True

        return False

    @staticmethod
    def has_ancestry(some_flow_1, some_flow_2):
        if FindFlowHierarchy(some_flow_1).is_ancestor_of(some_flow_2):
            return True

        return FindFlowHierarchy(some_flow_2).is_ancestor_of(some_flow_1)

    def parent_flow(self):
        """
        Purpose:
            Use the parent flow name for coarse-grained clustering

        Example:
            this flow:
                GOSS_CONTRACT_FIND_WITH_MCN
            becomes
                GOSS_CONTRACT_FIND

        Rationale:
            -   It's unlikely that grouping by actual flow names will create significant clusters.
            -   It's more likely a user will have multiple statements for
                    GOSS_CONTRACT_FIND
                which may incorporate
                    GOSS_CONTRACT_FIND_WITH_MCN
                    GOSS_CONTRACT_FIND_WITH_AVPN
                    GOSS_CONTRACT_FIND_BY_NUMBER
                etc
        """

        def _parent():
            flow_parent = FindFlowHierarchy(self.input).get_parent()
            if flow_parent:
                return flow_parent.upper()

            return self.input

        parent = _parent()
        if parent == "GOSS":
            return self.input

        self.logger.debug("\n".join([
            "Located Flow Parent",
            "\tFlow: {0}".format(self.input),
            "\tParent: {0}".format(parent)]))

        return parent
