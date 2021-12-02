#!/usr/bin/env python
# -*- coding: UTF-8 -*-


import os
import pprint

from pandas import DataFrame
from tabulate import tabulate

from base import BaseObject
from base import FileIO


class CountryRegionGenerator(BaseObject):

    def __init__(self,
                 df: DataFrame):
        """
        Created:
            20-May-2019
            craig.trim@ibm.com
        :param df:
            a DataFrame from 'geo-lookup.xlsx'
        """
        BaseObject.__init__(self, __name__)
        self.df = df

    def _build_lookup(self) -> dict:

        def _variants(some_text: str) -> list:
            if not some_text:
                return []
            return [x.strip() for x in some_text.split(",") if x]

        d_results = {}
        for country in self.df.Country.unique():
            df2 = self.df[self.df.Country == country]

            regions = df2.Region.unique()
            if regions is None or len(regions) == 0 or type(regions[0]) != str:
                continue

            if len(regions) > 1:
                raise ValueError("\n".join([
                    "Country/Region Ambiguity",
                    "\tCountry: {}".format(country),
                    "\tRegion: {}\n".format(regions),
                    tabulate(df2,
                             headers='keys',
                             tablefmt='psql')]))

            the_region = regions[0].lower()
            d_results[country.lower()] = the_region

            variants = df2.Variants.unique()
            if len(variants) and type(variants[0]) == str:
                for variant in _variants(variants[0]):
                    d_results[variant.lower()] = the_region

        return d_results

    def process(self):
        from taskmda.mda.dto import KbNames
        from taskmda.mda.dto import KbPaths
        from taskmda.mda.dmo import GenericTemplateAccess

        results = self._build_lookup()
        the_json_result = pprint.pformat(results, indent=4)
        the_json_result = "{0} = {{\n {1}".format(
            KbNames.country_to_region(), the_json_result[1:])

        the_template_result = GenericTemplateAccess.process()
        the_template_result = the_template_result.replace(
            "CONTENT", the_json_result)

        path = os.path.join(os.environ["CODE_BASE"],
                            KbPaths.country_to_region())
        FileIO.text_to_file(path, the_template_result)
