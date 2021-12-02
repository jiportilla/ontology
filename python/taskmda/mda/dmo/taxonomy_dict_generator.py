#!/usr/bin/env python
# -*- coding: UTF-8 -*-


from base import BaseObject


class TaxonomyDictGenerator(BaseObject):
    def __init__(self, some_lines):
        """
        Created:
            3-Apr-2017
            craig.trim@ibm.com
        Updated:
            17-Jul-2019
            craig.trim@ibm.com
            *   migrated from abacus-att
                git@github.ibm.com:abacus-implementation/abacus-att.git
        :param some_lines:
        """
        BaseObject.__init__(self, __name__)
        self.lines = some_lines

    # @staticmethod
    # def normalize(some_dict):
    #     the_normalized_dict = {}
    #     for key in some_dict:
    #         the_normalized_dict[key] = list(some_dict[key])
    #     return the_normalized_dict

    @staticmethod
    def tabs(some_line):
        return len(some_line.split("\t")) - 1

    def group_1(self):
        """
        Purpose:
            *   Create structured JSON from tab-delimited hierarchy

        Created:
            5-Apr-2017
            craig.trim@ibm.com
        :return: 
        """
        master = []

        temp = []
        for line in self.lines:

            line = line.replace("\t", "+")
            line = line.replace("    ", "+")

            total_tabs = len(line.split("+")) - 1
            temp.append(line)

            if 0 == total_tabs:
                master.append(temp)
                temp = []

        master.append(temp)
        return master

    def get_lines(self):
        lines = []
        for line in self.lines:
            line = line.replace("\t", "+")
            line = line.replace("    ", "+")
            lines.append(line.strip().lower())

        return lines

    @staticmethod
    def total(some_line):
        return some_line.count('+')

    def get_last_viable_line(self, lines, ctr, target):
        last_line = lines[ctr - 1]
        last_total = self.total(last_line)

        if last_total > target:
            return self.get_last_viable_line(lines, ctr - 1, target)

        return last_line

    @staticmethod
    def normalize(flow_name):
        return flow_name.replace("+", "").lower().strip()

    def process(self):
        d = {}

        ctr = 0
        lines = self.get_lines()

        for line in lines:
            total = self.total(line)
            if total > 0:
                parent = self.get_last_viable_line(lines, ctr, total - 1)

                _parent = self.normalize(parent)
                if _parent not in d:
                    d[_parent] = []
                d[_parent].append(self.normalize(line))

            ctr += 1

        return d
