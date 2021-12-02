#!/usr/bin/env python
# -*- coding: UTF-8 -*-


import unittest

from datagit.ingest.core.svc import FindRepoID


class FindRepoIDTest(unittest.TestCase):

    def test_unstructured_analytics(self):
        repo_id = FindRepoID(is_debug=True,
                             repo_owner="GTS-CDO",
                             repo_name="unstructured-analytics").process()
        self.assertEquals(repo_id, 508363)


if __name__ == '__main__':
    unittest.main()
