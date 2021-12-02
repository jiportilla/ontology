# !/usr/bin/env python
# -*- coding: UTF-8 -*-

import pytest

from nlutext import PerformSentenceSegmentation

def segmentation_test_cases():
    test_cases = [
        (
            'A blockchain,[1][2][3] originally block chain,[4][5] is a growing list of records',
            ['A blockchain, originally block chain, is a growing list of records']
        ),
        (
            'Greenfield Whole-Systems Enterprise Architecture.. '
            'I can write deep analytical scripts quickly but have spent many years architecting and implementing large scale and repeatable solutions.. '
            'I have written expert systems for Oil & Gas, Telco, Airlines, Automotive, and other industries.. '
            'Effectively deal with Data at Scale by applying deep semantics with Machine Learning and Graphs.. '
            'Dialog and Chat Bots, Deep Learning, Machine Learning, '
            'Data Mining, Enterprise Architecture, Data Analytics, '
            'Statistics, PySpark, Python, XSLT, Hadoop, UML, Docker, Jenkins, '
            'tensorflow, Java Enterprise Edition, DB2, Spark, Natural Language Processing, '
            'Data Science, Ontologies, WEKA, OWL, W3C, Technical Presentations, '
            'Provenance, RDFS, Solr, Elasticsearch, Lucene, word2vec, NLTK, '
            'spaCy, Cloud Computing, AWS, Google Cloud Compute, IBM Bluemix, '
            'NLC, Ubuntu Debian Linux, Red Hat Linux, CentOS',
            ['Greenfield Whole-Systems Enterprise Architecture.',
            'I can write deep analytical scripts quickly but have spent many years architecting and implementing large scale and repeatable solutions.',
            'I have written expert systems for Oil & Gas.',
            'Telco.',
            'Airlines.',
            'Automotive.',
            'and other industries.',
            'Effectively deal with Data at Scale by applying deep semantics with Machine Learning and Graphs.',
            'Dialog and Chat Bots.',
            'Deep Learning.',
            'Machine Learning.',
            'Data Mining.',
            'Enterprise Architecture.',
            'Data Analytics.',
            'Statistics.',
            'PySpark.',
            'Python.',
            'XSLT.',
            'Hadoop.',
            'UML.',
            'Docker.',
            'Jenkins.',
            'tensorflow.',
            'Java Enterprise Edition.',
            'DB Spark.',
            'Natural Language Processing.',
            'Data Science.',
            'Ontologies.',
            'WEKA.',
            'OWL.',
            'W3C. Technical Presentations.',
            'Provenance.',
            'RDFS.',
            'Solr.',
            'Elasticsearch.',
            'Lucene.',
            'word2vec.',
            'NLTK.',
            'spaCy.',
            'Cloud Computing.',
            'AWS.',
            'Google Cloud Compute.',
            'IBM Bluemix.',
            'NLC.',
            'Ubuntu Debian Linux.',
            'Red Hat Linux.',
            'CentOS']
        ),
    ]
    for index, test_case in enumerate(test_cases):
        yield index, test_case[0], test_case[1]


@pytest.mark.parametrize("index, input_text, segmented", segmentation_test_cases())
def test_segmentation(index, input_text, segmented):
    actual_segmented = PerformSentenceSegmentation().process(input_text)
    assert(actual_segmented == segmented)
