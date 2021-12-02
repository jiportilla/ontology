# !/usr/bin/env python
# -*- coding: UTF-8 -*-


from base import BaseObject
from datadict.core.os import the_city_to_region_dict
from datadict.core.os import the_country_to_region_dict


class FindGeo(BaseObject):
    """ One-Stop-Shop Service API for Geo queries """

    _d_city_region = the_city_to_region_dict
    _d_country_region = the_country_to_region_dict
    _city_missing_count_dict = {}
    _country_missing_count_dict = {}

    def __init__(self):
        """
        Created:
            20-May-2019
            craig.trim@ibm.com
        Updated:
            1-Aug-2019
            craig.trim@ibm.com
            *   simple updates to typing and logging
        Updated:
            12-September-2019
            abhbasu3@in.ibm.com
            *   collect missing count for city and country
            *   generate missing cities, countries csv
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/863
        """
        BaseObject.__init__(self, __name__)

    def find_region_by_country(self,
                               country: str,
                               search_in_str: bool = False) -> str or None:
        country = country.lower().strip()

        if country in self._d_country_region:
            result = self._d_country_region[country]
            if result == "north america":
                return "na"
            return result.lower()

        if search_in_str:
            for k in sorted(self._d_country_region, key=len, reverse=True):
                if str(k).lower() in country:
                    return self._d_country_region[k].lower()

        if country == "unknown":
            return "unknown"

        if country:
            self.logger.warning(f"Region Not Found: {country}")

    def normalize_country(self,
                          country: str,
                          search_in_str: bool = False) -> str or None:

        from cendalytics.geo.svc import GenerateMissingGeo

        country = country.lower().strip()

        if country in self._d_country_region:
            return country

        if search_in_str:
            for k in sorted(self._d_country_region, key=len, reverse=True):
                if str(k).lower() in country:
                    return str(k)

        if country:
            self.logger.warning(f"Country Not Found: {country}")
            # collect missing count
            GenerateMissingGeo.collect_missing_count("country", country)

    def find_region_by_city(self,
                            city: str,
                            search_in_str: bool = False) -> str or None:
        city = city.lower().strip()

        if city in self._d_city_region:
            result = self._d_city_region[city]
            if result == "north america":
                return "na"
            return result

        if search_in_str:
            for k in sorted(self._d_city_region, key=len, reverse=True):
                if str(k).lower() in city:
                    return self._d_city_region[k]

        if city:
            self.logger.warning(f"Region Not Found: {city}")

    def normalize_city(self,
                       city: str,
                       search_in_str: bool = False) -> str or None:

        from cendalytics.geo.svc import GenerateMissingGeo

        city = city.lower().strip()

        if city == "n a":
            return None
        if city == "none":
            return None

        if city in self._d_city_region:
            return city

        if search_in_str:
            for k in sorted(self._d_city_region, key=len, reverse=True):
                if str(k).lower() in city:
                    return str(k)

        if city:
            self.logger.warning(f"City Not Found: {city}")
            # collect missing count
            GenerateMissingGeo.collect_missing_count("city", city)
