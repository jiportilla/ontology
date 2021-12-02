import csv
import os

from base import BaseObject

GEO_BASE_PATH = "resources/output"

class GenerateMissingGeo(BaseObject):
    """ Auto Generate missing city or country csv"""
    
    _city_missing_count_dict = {}
    _country_missing_count_dict = {}

    def __init__(self):
        """
        Created:
            12-September-2019
            abhbasu3@in.ibm.com
            *   collect missing count for city and country
            *   generate missing cities, countries csv
                https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/863
        """
        BaseObject.__init__(self, __name__)

    @staticmethod
    def collect_missing_count(data_type, value):
        """
        Collect missing entry in country and city

        :param data_type: city/country
        :param value: value
        :return:
        """
        missing_dict = None
        if data_type == "city":
            missing_dict = GenerateMissingGeo._city_missing_count_dict
        if data_type == "country":
            missing_dict = GenerateMissingGeo._country_missing_count_dict

        if not missing_dict:
            missing_dict[value] = 1

        if value in missing_dict.keys():
            missing_dict[value] = missing_dict[value] + 1
        else:
            missing_dict[value] = 1

    @staticmethod
    def generate_missing_data(some_datatype):
        """
        generate csv for missing entry city/country counts

        :param some_datatype:
        :return:
        """
        missing_dict = None
        filename = None
        csv_file_path = os.path.join(os.environ["GTS_BASE"], GEO_BASE_PATH)

        if some_datatype == "city":
            filename = 'missing-city.csv'
            missing_dict = GenerateMissingGeo._city_missing_count_dict
        if some_datatype == "country":
            filename = 'missing-country.csv'
            missing_dict = GenerateMissingGeo._country_missing_count_dict

        csv_columns = ['name', 'missing-count']

        try:
            with open(csv_file_path + '/' + filename, 'w') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=csv_columns)
                writer.writeheader()
                for key, value in missing_dict.items():
                    writer.writerow({'name': key, 'missing-count': value})
        except IOError:
            raise IOError(f"I/O Error while generating geo csvs")
