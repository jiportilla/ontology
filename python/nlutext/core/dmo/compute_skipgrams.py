#!/usr/bin/env python
# -*- coding: UTF-8 -*-

import logging

from base import BaseObject


def to_list(results):
    """
    Purpose:
        Simplify the ComputeSkipGrams result set
    :param results:
        a ComputeSkipsGrams result set
        looks like this
            [(u'Problems', u'installing'), (u'Problems', u'adobe'), (u'Problems', u'acrobat'), ... ,]
    :return:
        a list of results
        looks like this
            ["Problems installing", "Problems adobe", "Problems acrobat", ... ,]
    """
    the_list = []
    for result in list(results):
        the_list.append(" ".join(list(result)))
    return the_list


class ComputeSkipGrams(BaseObject):
    def __init__(self):
        """
        Reference:
            <http://stackoverflow.com/questions/31847682/how-to-compute-skipgrams-in-python>
        """
        BaseObject.__init__(self, __name__)

    @staticmethod
    def pad_sequence(sequence, n, pad_left=False, pad_right=False, pad_symbol=None):
        from itertools import chain
        if pad_left:
            sequence = chain((pad_symbol,) * (n - 1), sequence)
        if pad_right:
            sequence = chain(sequence, (pad_symbol,) * (n - 1))
        return sequence

    def process(self, sequence, n, k, pad_left=False, pad_right=False, pad_symbol=None):
        from itertools import combinations
        sequence_length = len(sequence)
        sequence = iter(sequence)
        sequence = self.pad_sequence(sequence, n, pad_left, pad_right, pad_symbol)

        if sequence_length + pad_left + pad_right < k:
            raise Exception("The length of sentence + padding(s) < skip")

        if n < k:
            raise Exception("Degree of Ngrams (n) needs to be bigger than skip (k)")

        history = []
        nk = n + k

        # Return point for recursion.
        if nk < 1:
            return
        # If n+k longer than sequence, reduce k by 1 and recur
        elif nk > sequence_length:
            for ng in self.process(list(sequence), n, k - 1):
                yield ng

        while nk > 1:  # Collects the first instance of n+k length history
            history.append(next(sequence))
            nk -= 1

        # Iterative drop first item in history and picks up the next
        # while yielding skipgrams for each iteration.
        for item in sequence:
            history.append(item)
            current_token = history.pop(0)
            # Iterates through the rest of the history and
            # pick out all combinations the n-1grams
            for idx in list(combinations(range(len(history)), n - 1)):
                ng = [current_token]
                for _id in idx:
                    ng.append(history[_id])
                yield tuple(ng)

        # Recursively yield the skigrams for the rest of seqeunce where
        # len(sequence) < n+k
        for ng in list(self.process(history, n, k - 1)):
            yield ng
