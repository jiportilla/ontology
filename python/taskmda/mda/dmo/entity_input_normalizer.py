#!/usr/bin/env python
# -*- coding: UTF-8 -*-


import codecs

from base import BaseObject


class EntityInputNormalizer(BaseObject):
    """
    Purpose:
        The original entity YML file is passed in as a doc
        this component creates a modified (intermediary) CSV from this input
        the contents of the CSV are returned as a pandas DF

        The purpose of this component is to
            1.  performed sorting
            2.  de-duplication
            3.  other transformative routines
        with the purpose of providing consistency
    """

    def __init__(self, some_input_docs, some_output_file):
        """
        Updated:
            12-Apr-2017
            craig.trim@ibm.com
            *   renamed from "ProductKbProcessor"
        Updated:
            30-May-2017
            craig.trim@ibm.com
            *   updates reflect change from entity CSV => entity YML
        Updated:
            3-Jul-2017
            craig.trim@ibm.com
            *   added 'children' and 'parent'
        Updated:
            26-Jul-2017
            craig.trim@ibm.com
            *   migrated expansion logic into 'EntityPatternExpansion'
        Updated:
            1-Aug-2017
            craig.trim@ibm.com
            *   inject params
                <https://github.ibm.com/abacus-implementation/Abacus/issues/1721#issuecomment-3069169>
            *   renamed from 'GenerateModifiedEntities
            *   service logic removed and placed in 'generate-modified-entities'
        Updated:
            21-Feb-2019
            craig.trim@ibm.com
            *   migrated to text
        :param some_input_docs:
            the input YML docs
                expected format:
                    {
                        'entity-kb': <doc>,
                        'entity-cities-kb': <doc>
                        'entity-pts-kb': <doc>
                    }
        :param some_output_file:
            the modified CSV file prior to generation
                see comment #2 in the purpose section above
        """
        BaseObject.__init__(self, __name__)
        self.input_docs = some_input_docs
        self.output_file = some_output_file

    @staticmethod
    def get_patterns(some_entry):
        if "patterns" in some_entry:
            return some_entry["patterns"]
        return ""

    @staticmethod
    def get_children(some_entry):
        if "children" in some_entry:
            return ",".join(list(sorted(set(some_entry["children"]))))
        return ""

    @staticmethod
    def get_parent(some_entry):
        if "parent" in some_entry:
            return some_entry["parent"]
        return ""

    @staticmethod
    def get_params(some_entry):
        """
        :param some_entry:
        :return:
            the params in this format
                a = b, c = d, etc
        """
        if "params" not in some_entry:
            return ""

        param_buffer = ""

        ctr = 1
        max_params = len(some_entry["params"])

        for param_key in some_entry["params"]:
            param_value = str(some_entry["params"][param_key]).lower()

            if "false" == param_value:
                param_value = "no"
            elif "true" == param_value:
                param_value = "yes"

            param_buffer += "{0} = {1}".format(param_key, param_value)

            if ctr < max_params:
                param_buffer += ", "

            ctr += 1

        return param_buffer

    def normalize_input_doc(self, some_input_doc_name, some_input_doc, some_target):

        def _scoped(some_key):
            if "scoped" in some_input_doc[some_key]:
                return some_input_doc[some_key]["scoped"]
            return True

        for key in some_input_doc:
            the_type = some_input_doc[key]["type"]
            is_scoped = _scoped(key)
            the_patterns = self.get_patterns(some_input_doc[key])
            the_params = self.get_params(some_input_doc[key])
            the_children = self.get_children(some_input_doc[key])
            the_parent = self.get_parent(some_input_doc[key])

            some_target.write(u"{0}|{1}|{2}|{3}|{4}|{5}|{6}|{7}\n".format(
                key, the_type, the_params, is_scoped, the_patterns, the_children, the_parent, some_input_doc_name))

    def create_modified_csv(self):
        """
        Purpose:
            - iterate the input YML doc
            - apply transformative routines
            - write the output (modified) CSV
        """

        target = codecs.open(self.output_file, "w", encoding="utf-8")
        for input_doc_name in self.input_docs:
            input_doc = self.input_docs[input_doc_name]
            self.normalize_input_doc(input_doc_name, input_doc, target)

        target.close()

    def process(self):
        """
        :return:
            a pandas dataframe
        """
        from taskmda.mda.dmo import EntityKbReader

        self.create_modified_csv()

        return EntityKbReader(self.output_file).read_csv()
