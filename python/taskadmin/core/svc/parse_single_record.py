#!/usr/bin/env python
# -*- coding: UTF-8 -*-


import pprint

from base import BaseObject
from datamongo import CendantCollection
from nlutext import MultiTextParser

IS_DEBUG = True


class ParseSingleRecord(BaseObject):

    def __init__(self,
                 ontology_names: list,
                 is_debug: bool = False):
        """
        Updated:
            20-Mar-2019
            craig.trim@ibm.com
            *   moved out of a 'scripts' directory and cleaned up
        Update:
            17-Feb-2020
            craig.trim@ibm.com
            *   use multiple ontologies as input
        """
        BaseObject.__init__(self, __name__)
        self._is_debug = is_debug

        if not ontology_names or not len(ontology_names):
            ontology_names = ['base']

        if self._is_debug:
            self.logger.debug('\n'.join([
                "Instantiated ParseSingleRecord",
                f"\tOntologies: {ontology_names}"]))

        self.text_parser = MultiTextParser(is_debug=is_debug,
                                           ontology_names=ontology_names)

    def process(self,
                some_text: str) -> dict:
        results = self.text_parser.process(some_text)
        self.logger.debug(f"Completed in {self.timer.end()}s")

        return results

    def random(self) -> dict:
        collection = CendantCollection(some_db_name="cendant",
                                       some_collection_name="cv_parse_employee")

        record = collection.random(total_records=1)[0]

        value = [field["value"] for field in record["fields"]
                 if field["type"] == "long-text"][0]
        value = " ".join(value)

        self.logger.debug("\n".join([
            "Retrieved Random Record",
            "\ttext: {}".format(value)]))

        return self.process(value)


def main(text, ontology_name):
    def _ontologies() -> list:
        if not ontology_name:
            return ['base']
        if ontology_name == 'biotech':
            return ['base', 'biotech']
        return [ontology_name]

    def _result():
        if text == "random":
            return ParseSingleRecord(is_debug=IS_DEBUG,
                                     ontology_names=_ontologies()).random()
        return ParseSingleRecord(is_debug=IS_DEBUG,
                                 ontology_names=_ontologies()).process(text)

    svcresult = _result()

    print("\n\n\n----------------------------------------------------")
    pprint.pprint(svcresult, indent=4)
    print("----------------------------------------------------\n\n\n")

    print(svcresult["ups"]["normalized"])


if __name__ == "__main__":
    import plac

    plac.call(main)
