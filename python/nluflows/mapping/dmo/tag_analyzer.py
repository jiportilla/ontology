#!/usr/bin/env python
# -*- coding: UTF-8 -*-

from base import BaseObject

term_discrimination = {
    "high": ["access failure", "cap", "vdi", "order history", "cardsprod", "cardsprod false",
             "access network drive failure", "map drive failure"],
    "low": ["password", "mailbox", "reset", "request"]}


class TagAnalyzer(BaseObject):
    """
    Purpose:
        compute the difference between required tags and user tags
        example:
            given:
                include_all_of = { alpha1, alpha2 }
                user_tags = { alpha1, alpha2, alpha3 }
            the difference would be 0
            all the required tags exist in the user tag set
    Updated:
        17-Jul-2019
        craig.trim@ibm.com
        *   migrated from abacus-att
            git@github.ibm.com:abacus-implementation/abacus-att.git
    """

    def __init__(self,
                 some_tags,
                 some_candidate_mapping,
                 some_candidate_name,
                 debug: bool = False):
        BaseObject.__init__(self, __name__)
        self.tags = some_tags
        self.candidate_mapping = some_candidate_mapping
        self.candidate_name = some_candidate_name
        self.debug = debug

    def compute_inclusion(self, some_entry, some_type):
        """
        Updated:
            2017-04-03
            craig.trim@ibm.com
            *   major computation updates around inclusion deductions
                reference SEV-1 #1343

        :param some_entry:
        :param some_type:
        :return:    the intersection (how many required tags exist)
                    and difference (how many required tags are missing)
        """
        required_tags = set(some_entry[some_type])
        if 0 == len(required_tags):
            return [], []

        difference = list(required_tags.difference(self.tags))
        intersection = list(required_tags.intersection(self.tags))

        if self.debug:
            self.logger.debug(
                "Inclusion Computation Complete (type = {0}):\n\tintersection = {1}\n\tdifference = {2}".format(
                    some_type, intersection, difference))

        return intersection, difference

    def compute_include_one_of(self, some_entry):
        """
        compute whether the requirements for 'include_one_of' are met
        :param some_entry:
        :return: True if at least one user tag matches any single tag in 'include_one_of'
        """
        required_tags = set(some_entry["include_one_of"])
        if 0 == len(required_tags):
            return None

        for user_tag in self.tags:
            if user_tag in required_tags:
                return True  # only one tag needs to match

        # no tags match
        return False

    def compute_exclude_all_of(self, some_entry):
        """
        compute whether the requirements for 'exclude-all-of' are met
        :param some_entry:
        :return:    True iif the user tag set contains all tags in 'exclude-all-of'
                    otherwise False
        """
        required_tags = set(some_entry["exclude_all_of"])
        if 0 == len(required_tags):
            return False

        for required_tag in required_tags:
            if required_tag not in self.tags:
                return False

        # all tags match
        return True

    def compute_exclude_one_of(self, some_entry):
        """
        compute whether the requirements for 'exclude-one-of' are met
        :param some_entry:
        :return:    True iif at least one user tag exists in this set
                    otherwise False
        """
        required_tags = set(some_entry["exclude_one_of"])
        if 0 == len(required_tags):
            return False

        for user_tag in self.tags:
            if user_tag in required_tags:
                return True

        # no tags match
        return False

    def is_discriminatory_generic(self, some_inclusion_list):
        boost = 0

        for user_tag in self.tags:
            if user_tag in some_inclusion_list:
                if user_tag in term_discrimination["high"]:
                    boost += 1
                elif user_tag in term_discrimination["low"]:
                    boost -= 1

        return boost

    def is_discriminatory(self, some_entry):
        boost = 0

        boost += self.is_discriminatory_generic(
            set(some_entry["include_all_of"]))
        boost += self.is_discriminatory_generic(
            set(some_entry["include_one_of"]))

        return boost

    @staticmethod
    def get_deduction(some_entry):
        """
        :param some_entry:
        :return:
        """

        if not some_entry["deduction"]:
            return 0

        deduction = some_entry["deduction"]
        if deduction > 100:
            return 100
        if deduction < 0:
            return 0
        return deduction

    def analyze_include_one_of(self, some_entry):

        include_one_of = set(some_entry["include_one_of"])
        total_matches = 0
        for user_tag in self.tags:
            if user_tag in include_one_of:
                total_matches += 1

        is_required = len(include_one_of) > 0

        return {"required": is_required,
                "matched": total_matches}

    def get_exclusivity(self, some_entry):
        is_required = some_entry["exclusive"]
        if not is_required:
            return {
                "required": False,
                "matched": False
            }

        total = len(some_entry["include_all_of"])
        if len(some_entry["include_one_of"]) > 1:
            total += 1

        return {
            "required": True,
            "matched": (total == len(self.tags))
        }

    def process(self):
        include_one_of_intersection, include_one_of_difference = self.compute_inclusion(
            self.candidate_mapping, "include_one_of")

        include_all_of_intersection, include_all_of_difference = self.compute_inclusion(
            self.candidate_mapping, "include_all_of")

        exclude_one_of_intersection, exclude_one_of_difference = self.compute_inclusion(
            self.candidate_mapping, "exclude_one_of")

        exclude_all_of_intersection, exclude_all_of_difference = self.compute_inclusion(
            self.candidate_mapping, "exclude_all_of")

        include_one_of_match = self.compute_include_one_of(
            self.candidate_mapping)
        is_direct_match = (0 == len(include_all_of_difference)
                           ) and include_one_of_match
        is_exclude_all_of = self.compute_exclude_all_of(self.candidate_mapping)
        is_exclude_one_of = self.compute_exclude_one_of(self.candidate_mapping)
        deduction = self.get_deduction(self.candidate_mapping)

        return {
            "process": self.candidate_name,
            "direct-match": is_direct_match,
            "analysis": {
                "exclusivity": self.get_exclusivity(self.candidate_mapping),
                "include_one_of": {
                    "total": self.candidate_mapping["include_one_of"],
                    "missing": include_one_of_difference,
                    "contains": include_one_of_intersection
                },
                "include_all_of": {
                    "total": self.candidate_mapping["include_all_of"],
                    "missing": include_all_of_difference,
                    "contains": include_all_of_intersection
                },
                "exclude_one_of": {
                    "total": self.candidate_mapping["exclude_one_of"],
                    "missing": exclude_one_of_difference,
                    "contains": exclude_one_of_intersection
                },
                "exclude_all_of": {
                    "total": self.candidate_mapping["exclude_all_of"],
                    "missing": exclude_all_of_difference,
                    "contains": exclude_all_of_intersection
                },
                "flags": {
                    # "fails-exclusivity-test": fails_exclusivity_test,
                    "deduction": deduction,
                    "discriminatory": self.is_discriminatory(self.candidate_mapping)
                }
            }
        }
