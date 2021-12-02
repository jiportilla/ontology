#!/usr/bin/env python
# -*- coding: UTF-8 -*-


import unittest

from nlusvc import RemoveStopWords

IS_DEBUG = True


class TestRemoveStopWords(unittest.TestCase):

    def execute(self,
                some_input: str or list,
                expected_result: str or list) -> None:
        rsw = RemoveStopWords(is_debug=True)
        actual_result = rsw.process(input_text=some_input,
                                    aggressive=False)
        self.assertEqual(actual_result, expected_result)

    def test_process(self):
        self.execute(["and plan to learn"],
                     ["plan learn"])

        self.execute("and plan to learn",
                     "plan learn")

        self.execute("and and plan to to learn",
                     "plan learn")

        self.execute("to and and plan to to learn and to and to plan to and",
                     "plan learn plan")

        self.execute("plan andlearn toplan",
                     "plan andlearn toplan")

        self.execute(
            "apply learn orient business_analyst which include analytics a client have plan train and no_skills_growth human_performance and delivery requirement which is follow by create plan strategy for a comprehensive approach to human_performance increase",
            "learn orient business_analyst analytics client plan train no_skills_growth human_performance delivery requirement create plan strategy comprehensive approach human_performance increase")


if __name__ == '__main__':
    unittest.main()
