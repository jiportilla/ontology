#!/usr/bin/env python
# -*- coding: UTF-8 -*-


import unittest

from pandas import DataFrame
from tabulate import tabulate

from nlusvc import ExpansionAPI


class TestExpansionAPI(unittest.TestCase):
    _api = ExpansionAPI(is_debug=False)

    def test_10_instances(self):
        """
        Expected Output:
        +----+-----------------------------------------+----------+-------------------------+
        |    | Key                                     | Type     | Value                   |
        |----+-----------------------------------------+----------+-------------------------|
        |  0 | Enterprise Architecture and Development | Instance | ['Java', ',', 'Python'] |
        |  1 | IBM Master Inventor                     | Instance | ['85th', 'Plateau']     |
        +----+-----------------------------------------+----------+-------------------------+
        """
        source_text = """
            Enterprise Architecture and Development (Java, Python)
            Focused on Software Usability, Adoption and Gamification
            Deep Statistical Background
            IBM Master Inventor (85th Plateau)
            Open Group Master IT Specialist
            W3C PROV Working Group Member
        """
        result = self._api.instances(source_text)
        print(tabulate(result,
                       headers='keys',
                       tablefmt='psql'))

        self.assertEqual(type(result), DataFrame)
        self.assertEqual(len(result), 2)

    def test_20_acronyms(self):
        """
        Expected Output:
        +----+-------+---------+-----------------------------+
        |    | Key   | Type    | Value                       |
        |----+-------+---------+-----------------------------|
        |  0 | TF    | Acronym | Tensorflow                  |
        |  1 | NLP   | Acronym | Natural Language Processing |
        |  2 | ML    | Acronym | Machine Learning            |
        |  3 | BPM   | Acronym | Business Process Modeling   |
        +----+-------+---------+-----------------------------+
        """
        source_text = """
            Deep Learning (RNNs, CNNs), Tensorflow (TF), Natural Language Processing (NLP),
            Machine Learning (ML), Statistics, Probabilistic Modeling, Information Management,
            Triple Stores, Property Graphs, Relational DB Design (RDB Design),
            W3C Standards, Agile Method
            Wrangling data at scale
            IBM SOA and OMG Business Process Modeling (BPM) Certified
        """.strip()

        result = self._api.acronyms(source_text)
        print(tabulate(result,
                       headers='keys',
                       tablefmt='psql'))

        self.assertEqual(type(result), DataFrame)
        self.assertEqual(len(result), 4)

        _types = result.Type.unique()
        self.assertEqual(len(_types), 1)
        self.assertEqual("Acronym", _types[0])

        _values = result.Value.unique()
        self.assertEqual(len(_values), 4)
        self.assertTrue("Tensorflow" in _values)

    def unplug_test_30_explain(self):
        source_text = """
            Deep Learning (RNNs, CNNs), Tensorflow (TF), Natural Language Processing (NLP),
            Machine Learning (ML), Statistics, Probabilistic Modeling, Information Management,
            Triple Stores, Property Graphs, Relational DB Design (RDB Design),
            W3C Standards, Agile Method
            Wrangling data at scale
            IBM SOA and OMG Business Process Modeling (BPM) Certified
            Enterprise Architecture and Development (Java, Python)
            Focused on Software Usability, Adoption and Gamification
            Deep Statistical Background
            IBM Master Inventor (85th Plateau)
            Open Group Master IT Specialist
            W3C PROV Working Group Member
        """.strip()

        result1 = self._api.all(source_text)
        print(tabulate(result1,
                       headers='keys',
                       tablefmt='psql'))

        result2 = self._api.explain(result1)
        print(tabulate(result2,
                       headers='keys',
                       tablefmt='psql'))


if __name__ == '__main__':
    unittest.main()
