#!/usr/bin/env python
# -*- coding: UTF-8 -*-


from base import BaseObject


class GenerateSlotQuery(BaseObject):
    """ Generate a Dimension Slot Query
    """

    _records = None

    def __init__(self,
                 is_debug: bool = False):
        """
        Created:
            21-May-2019
            craig.trim@ibm.com
            *   refactored out of '-dimemsions'
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/304\
        Updated:
            7-Oct-2019
            craig.trim@ibm.com
            *   change all operators to 'lt' and 'gt'
                e.g., we want "> 0" or "< 0" not ">= 0" or "<= 0"
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1048
        """
        BaseObject.__init__(self, __name__)
        self.is_debug = is_debug

    def process(self,
                region: str,
                slot_name: str,
                minimum_value_sum: float = None,
                maximum_value_sum: float = None,
                minimum_band_level: int = None,
                maximum_band_level: int = None) -> dict:

        def value_and_band_range():
            """
            Generate Query for Value Range and Band Range
            :return:
                a mongoDB query
            """

            _value_field = 'slots.{0}.z_score_norm'.format(slot_name)
            _low_band_field = 'fields.band.low_band'.format(slot_name)
            _high_band_field = 'fields.band.high_band'.format(slot_name)

            if minimum_band_level and maximum_band_level:  # and minimum_band_level != maximum_band_level:
                return {'$and': [
                    {_value_field: {'$gt': minimum_value_sum}},
                    {_value_field: {'$lt': maximum_value_sum}},
                    {_low_band_field: {'$gte': minimum_band_level}},
                    {_high_band_field: {'$lte': maximum_band_level}}]}
            elif minimum_band_level:
                return {'$and': [
                    {_value_field: {'$gt': minimum_value_sum}},
                    {_value_field: {'$lt': maximum_value_sum}},
                    {_low_band_field: {'$gte': minimum_band_level}}]}
            elif maximum_band_level:
                return {'$and': [
                    {_value_field: {'$gt': minimum_value_sum}},
                    {_value_field: {'$lt': maximum_value_sum}},
                    {_high_band_field: {'$lte': maximum_band_level}}]}
            elif not maximum_band_level and not minimum_band_level:
                return {'$and': [
                    {_value_field: {'$gt': minimum_value_sum}},
                    {_value_field: {'$lt': maximum_value_sum}}]}

        def value_and_band(a_zscore_norm: float,
                           is_greater_than: bool) -> dict:
            """
            Generate a Partial Value Range Query with Band Level
            :param a_zscore_norm:
            :param is_greater_than:
            :return:
                a mongoDB query
            """

            def _operand():
                if is_greater_than:
                    return '$gte'
                return '$lte'

            _value_field = 'slots.{0}.z_score_norm'.format(slot_name)
            _low_band_field = 'fields.band.low_band'.format(slot_name)
            _high_band_field = 'fields.band.high_band'.format(slot_name)

            if minimum_band_level and maximum_band_level:  # and minimum_band_level != maximum_band_level:
                return {'$and': [
                    {_value_field: {_operand(): a_zscore_norm}},
                    {_low_band_field: {'$gte': minimum_band_level}},
                    {_high_band_field: {'$lte': maximum_band_level}}]}
            elif maximum_band_level:
                return {'$and': [
                    {_value_field: {_operand(): a_zscore_norm}},
                    {_low_band_field: {'$lte': maximum_band_level}}]}
            elif minimum_band_level:
                return {'$and': [
                    {_value_field: {_operand(): a_zscore_norm}},
                    {_low_band_field: {'$gte': minimum_band_level}}]}
            elif not minimum_band_level and not maximum_band_level:
                return {_value_field: {_operand(): a_zscore_norm}}
                # return {'$and': [
                #     {_value_field: {_operand(): a_zscore_norm}}]}

        def _query():
            if minimum_value_sum and maximum_value_sum:
                return value_and_band_range()
            elif minimum_value_sum and not maximum_value_sum:
                return value_and_band(a_zscore_norm=minimum_value_sum,
                                      is_greater_than=True)
            elif maximum_value_sum and not minimum_value_sum:
                return value_and_band(a_zscore_norm=maximum_value_sum,
                                      is_greater_than=False)
            else:
                _value_field = 'slots.{0}.z_score_norm'.format(slot_name)
                return {_value_field: {'$gt': 0.0}}
                # return {'$and': [
                #     {_value_field: {'$gt': 0.0}}]}

        d_query = _query()
        if region:
            d_query['$and'].append({"fields.region": region})

        return d_query
