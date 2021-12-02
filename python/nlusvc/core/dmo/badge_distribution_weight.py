#!/usr/bin/env python
# -*- coding: UTF-8 -*-


from base import BaseObject
from base import FileIO


class AnnotationWeight(BaseObject):

    def __init__(self,
                 ontology_name: str = 'base',
                 is_debug: bool = False):
        """
        Created:
            17-Oct-2019
            craig.trim@ibm.com
            *   based on 'field-text-weight'
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1142#issuecomment-15377510
        """
        BaseObject.__init__(self, __name__)
        from nlusvc.core.bp import TextAPI

        self._is_debug = is_debug
        self._ontology_name = ontology_name
        self._text_api = TextAPI(is_debug=is_debug,
                                 ontology_name=ontology_name)

        config = self._config()

        self._exact_matches = [entry for entry in config
                               if entry['type'] == 'exact']

        self._partial_matches = [entry for entry in config
                                 if entry['type'] == 'partial']

    @staticmethod
    def _config() -> list:
        doc = FileIO.file_to_yaml_by_relative_path(
            "resources/config/weights/annotation_weights.yml")
        return doc["keys"]

    def process(self,
                some_annotation: str) -> float:
        some_annotation = some_annotation.lower().strip()

        for entry in self._exact_matches:
            if entry['value'].lower() == some_annotation:

                if self._is_debug:
                    self.logger.debug('\n'.join([
                        f"Exact Match Located "
                        f"(tag={some_annotation}, weight={entry['weight']})"]))

                return entry['weight']

        for entry in self._partial_matches:
            if self._text_api.has_match(a_token=entry['value'],
                                        some_text=some_annotation):
                if self._is_debug:
                    self.logger.debug('\n'.join([
                        f"Partial Match Located "
                        f"(tag={entry['value']}, text={some_annotation}, weight={entry['weight']})"]))

                return entry['weight']

        return 1.0
