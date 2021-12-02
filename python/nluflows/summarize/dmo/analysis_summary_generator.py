#!/usr/bin/env python
# -*- coding: UTF-8 -*-


from base import BaseObject


class AnalysisSummaryGenerator(BaseObject):
    def __init__(self, some_analyses):
        """
        given sample input:
        [
            {   'process': 'UC4_SC5_NETWORK_DRIVE-ALPHAT1',
                'analysis': {
                    'include-all-of': {
                        'contains': ['alphatag1', 'network drive'],
                        'missing': ['alphatag2'],
                        'total': ['alphatag1', 'alphatag2', 'network drive']
                    },
                    'include-one-of': True
                },
                'confidence': 67.0,
                'direct-match': False},

            {   'process': 'UC4_SC5_NETWORK_DRIVE-LOW_DISK_SPACE',
                'analysis': {
                    'include-all-of': {
                        'contains': ['network drive'],
                        'missing': ['hard disk space'],
                        'total': ['hard disk space', 'network drive']
                    },
                    'include-one-of': True
                },
                'confidence': 50.0,
                'direct-match': False}]

        return:
        {
            67: ['UC4_SC5_NETWORK_DRIVE'],
            50: ['UC4_SC5_NETWORK_DRIVE-LOW_DISK_SPACE']
        }
        Updated:
            17-Jul-2019
            craig.trim@ibm.com
            *   migrated from abacus-att
                git@github.ibm.com:abacus-implementation/abacus-att.git
        :param some_analyses:
        """
        BaseObject.__init__(self, __name__)
        self.analyses = some_analyses

    @staticmethod
    def normalize(some_dict):
        ndict = {}
        for key in some_dict:
            ndict[key] = list(some_dict[key])
        return ndict

    def process(self):
        rdict = {}

        for analysis in self.analyses:
            confidence = analysis["confidence"]
            if confidence not in rdict:
                rdict[confidence] = set()
            rdict[confidence].add(analysis["process"])

        return self.normalize(rdict)
