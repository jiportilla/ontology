# !/usr/bin/env python
# -*- coding: UTF-8 -*-


from base import BaseObject


class GraphNodeIdGenerator(BaseObject):
    """ Build a GitHub Node for Graphviz """

    def __init__(self,
                 a_type: str,
                 a_label: str,
                 is_debug: bool = True):
        """
        Created:
            19-Dec-2019
            craig.trim@ibm.com
            *   refactored out of 'graph-github-issue'
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1631
        Updated:
            31-Dec-2019
            craig.trim@ibm.com
            *   renamed from 'node-id-generator'
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1681#issuecomment-16877277
        :param is_debug:
            if True     increase log output at DEBUG level
        """
        BaseObject.__init__(self, __name__)
        self._type = a_type
        self._label = a_label
        self._is_debug = is_debug

    def process(self) -> str:
        a_type = self._type.lower().strip()

        a_label = self._label.upper().strip()
        a_label = a_label.replace('.', '_')
        a_label = a_label.replace('-', '_')
        a_label = a_label.replace('=', '_')
        a_label = a_label.replace('/', '_')

        def generate_label() -> str:
            if a_type == "issue":
                return f"NODE_ISSUE_{a_label}"
            elif a_type == "pull_request":
                return f"NODE_PR_{a_label}"
            elif a_type == "commit":
                return f"NODE_COMMIT_{a_label}"
            elif a_type == "file_commit":
                return f"NODE_FILE_COMMIT_{a_label}"
            elif a_type == "file_folder":
                return f"PYTHON_FILE_FOLDER_{a_label}"
            elif a_type == "file_path":
                return f"PYTHON_FILE_PATH_{a_label}"
            elif a_type == "file_name":
                return a_label
            elif a_type == "person":
                return f"PERSON_{a_label}"
            raise NotImplementedError

        def make_hash(some_label: str) -> str:
            # return f"GN_{BaseHash.hash(some_label)}"
            return f"GN_{some_label}"

        label = generate_label()
        if not label:
            raise NotImplementedError

        return make_hash(label)
