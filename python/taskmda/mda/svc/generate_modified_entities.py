#!/usr/bin/env python
# -*- coding: UTF-8 -*-


from base import BaseObject
from base import FileIO


class GenerateModifiedEntities(BaseObject):
    """
        Purpose:

            1.  we started with a 'entity_kb.csv' file
                and this initial CSV represented the semantic model
                    and was modified by the developers

            2.  this initial CSV was transformed automatically into a 'modified CSV'
                    the modified CSV performed sorting, de-duplication, and a few other transformative routines
                    aimed at providing consistency

            3.  the modified CSV was then transformed into a JSON dictionary

            4.  since then, we've moved to an 'entity_kb.yml' file
                    this is easier for developers to maintain and modify

            5.  however, the rest of the process remains unchanged
                    this YML file is transformed and saved as a 'modified CSV'
                    and a JSON dictionary is created from it
    """

    def __init__(self, some_input_files, some_output_file):
        """
        Created:
            1-Aug-2017
            craig.trim@ibm.com
            *   service logic refactored out of 'entity-kb-processor'
        Updated:
            21-Feb-2019
            craig.trim@ibm.com
            *   migrated to text
        :param some_input_files:
            the input file into the process 
                e.g. an entity YML file
                cardinality 1..*
        :param some_output_file:
            the modified CSV file prior to generation
                see comment #2 in the purpose section above
        """
        BaseObject.__init__(self, __name__)

        if some_input_files is None:
            raise ValueError
        if some_output_file is None:
            raise ValueError

        self.input_files = some_input_files
        self.output_file = some_output_file

    @staticmethod
    def get_input_file_name(some_input_file):
        name = some_input_file.split("/")[-1].split(".")[0].strip()
        name = name.replace("_", "-").replace("-kb", "")

        return name

    def to_docs(self):
        from taskmda.mda.dmo import EntityPatternExpansion

        docs = {}
        for some_input_file in self.input_files:
            input_file_name = self.get_input_file_name(some_input_file)

            doc = FileIO.file_to_yaml(some_input_file)
            doc = EntityPatternExpansion(doc).process()

            docs[input_file_name] = doc

        return docs

    def process(self):
        from taskmda.mda.dmo import EntityInputNormalizer

        docs = self.to_docs()
        df = EntityInputNormalizer(docs, self.output_file).process()

        self.logger.info("Generated Modified Dataframe (len = {0})".format(
            len(df)))

        return df
