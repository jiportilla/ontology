#!/usr/bin/env python
# -*- coding: UTF-8 -*-


import codecs
import os
import pprint

from base import BaseObject


class CleanSynonyms(BaseObject):
    """ Analyzes the Synonyms CSV file for errors

        Common Errors:
            1.  Double Assignment:
                    alpha~gamma
                    beta~gamma

            2.  Circular Assignment:
                    beta~betas
                    betas~beta

        Sort and Merge:
            this:
                alpha~gamma, beta, gamma, beta
            becomes:
                alpha~beta, gamma """

    def __init__(self,
                 file_name: str):
        """
        Updated:
            20-Mar-2019
            craig.trim@ibm.com
            *   moved out of a 'scripts' directory and cleaned up
        Update:
            15-Jan-2020
            craig.trim@ibm.com
            *   add file-name to param list and modify both biotech and cendant files
        """
        BaseObject.__init__(self, __name__)
        self._file_name = file_name

    def _lines(self) -> list:
        path = os.path.join(os.environ["CODE_BASE"],
                            f"resources/nlu/rels/{self._file_name}.csv")
        target = codecs.open(path, "r", encoding="utf-8")
        lines = target.readlines()
        target.close()

        return lines

    def _write_to_file(self,
                       syns: dict):
        path = os.path.join(os.environ["CODE_BASE"],
                            f"resources/nlu/rels/{self._file_name}.csv")

        target = codecs.open(path, "w", encoding="utf-8")
        for key in sorted(syns.keys()):
            values = syns[key]
            target.write("{}~{}\n".format(key, ", ".join(values)))
        target.close()

    @staticmethod
    def _sort_and_merge(lines: list) -> dict:
        """
        :param lines:
            the current lines the synonyms_kb.csv file
        :return:
            a dictionary that sorts and merges all these lines into a single dictionary

            for exmaple
                alpha~beta
                alpha~gamma
            becomes
                { alpha: [beta, gamma] }
        """
        d = {}
        for line in lines:
            key = line[0].lower().strip()

            key = key.replace(" ", "_")

            if len(line) != 2:
                raise ValueError("\n".join([
                    "Unrecognized Line Length",
                    "\texpected: 2",
                    "\tactual: {}".format(len(line)),
                    "\t{}".format(line)]))

            if not len(key):
                raise ValueError("\n".join([
                    "No Canonical Form Found",
                    "\tline: {}".format(line)]))

            values = sorted(set([x.lower().strip() for x in line[1].split(",") if x]))
            values = [x for x in values if x != key]
            values = [x for x in values if type(x) == str]

            if key.endswith("_skill"):
                values.append("expert {}".format(key[:len(key) - 6]))
                values.append(key.replace("_skill", " background"))
                values.append(key.replace("_skill", " experience"))

            if key in d:
                d[key] = set(set(d[key]).union(set(values)))
            else:
                d[key] = values

        for k in d:
            d[k] = sorted(set(d[k]))

        return d

    @staticmethod
    def _circular_assignment(d_syns: dict) -> None:
        """
        find contradictions in synonym definitions

        for example:
            beta~betas
            betas~beta

        throw an exception and refuse to proceed further until these are resolved
        :param d_syns:
        """

        def _error(value_1: str,
                   value_2: str):
            raise ValueError("\n".join([
                "Synonym Contradiction",
                "\tkey-1: {}".format(value_1),
                "\tkey-1-values: {}".format(d_syns[value_1]),
                "\tkey-2: {}".format(value_2),
                "\tkey-2-values: {}".format(d_syns[value_2]),
            ]))

        for k1 in d_syns:
            for k2 in d_syns:

                if k1 == k2:
                    continue
                elif k1 in d_syns[k2] and k2 in d_syns[k1]:
                    _error(k1, k2)

    @staticmethod
    def _double_assignment(d_syns: dict) -> list:
        """
        find ambiguity in synonym definitions

        for example:
            alpha~gamma
            beta~gamma

        log these amguitities
        :param d_syns:
        """

        def _error(value_1: str,
                   value_2: str,
                   ambiguous_values: set):
            return ("\n".join([
                "Synonym Ambiguity",
                "\tkey-1: {}".format(value_1),
                "\tkey-1-values: {}".format(d_syns[value_1]),
                "\tkey-2: {}".format(value_2),
                "\tkey-2-values: {}".format(d_syns[value_2]),
                "\tambiguous-value(s): {}".format(sorted(ambiguous_values))
            ]))

        errors = []
        for k1 in d_syns:
            k1_values = set(d_syns[k1])

            for k2 in d_syns:
                if k1 == k2:
                    continue

                k2_values = set(d_syns[k2])
                result = set(k1_values.intersection(k2_values))
                if len(result) > 0:
                    errors.append(_error(k1, k2, result))

        return errors

    def _numerical_values(self,
                          d_syns: dict) -> None:
        """
        Ingestion of dbPedia redirects as synonyms has caused some synonym entries to have numerical values
        for example:
            apple_workgroup_server~110, 120, macintosh_server_g3, macintosh_server_g4, etc
        these all need to be removed
        :param d_syns:
        """
        d_error = {}
        for k in d_syns:
            values = d_syns[k]
            values = [v for v in values if type(v) != str]
            if len(values):
                d_error[k] = values

        if len(d_error):
            self.logger.error("\n".join([
                "Numerical Values Located (total={})".format(len(
                    d_error)),
                pprint.pformat(d_error, indent=4)]))
            raise ValueError("Process Halted")

    @staticmethod
    def _remove_useless_values(d_syns: dict) -> None:
        """
        Ingestion of dbPedia redirects as synonyms has caused some synonym entries to have values useless for our purposes
        for example:
            alpha~alpha_(disambiguation), alpha_(film), alpha_(comics), etc
        any variant with a (...) will be removed
        :param d_syns:
        """
        for k in d_syns:
            values = d_syns[k]
            values = [v for v in values if '(' not in v]
            d_syns[k] = values

    def process(self):

        def _valid(x):
            if x.strip().startswith("#"):
                return False
            return True

        lines = self._lines()
        lines = [x.strip() for x in lines if _valid(x)]
        lines = [x.replace(";", ",").split("~") for x in lines if x]

        d_syns = self._sort_and_merge(lines)

        self._numerical_values(d_syns)
        self._circular_assignment(d_syns)
        self._remove_useless_values(d_syns)

        ambiguity_errors = self._double_assignment(d_syns)

        self._write_to_file(d_syns)

        if len(ambiguity_errors):
            print("Total Ambiguity Errors: {}".format(len(ambiguity_errors)))
            pprint.pprint(ambiguity_errors)


def main():
    CleanSynonyms('synonyms_cendant_kb').process()
    CleanSynonyms('synonyms_biotech_kb').process()


if __name__ == "__main__":
    main()
