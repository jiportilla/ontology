#!/usr/bin/env python
# -*- coding: UTF-8 -*-


import codecs
import io
import json
import os
from collections import Counter

import jsonpickle
import yaml
from yaml import Loader


class FileIO(object):
    """methods for File I/O"""

    @staticmethod
    def absolute_path(file_name: str,
                      validate: bool = True):

        path = os.path.join(os.environ["CODE_BASE"],
                            "resources",
                            file_name)

        if validate and not os.path.isfile(path):
            raise ValueError("\n".join([
                "File Not Found",
                f"\tPath: {path}"]))

        return path

    @staticmethod
    def csv_to_file(out_file_path: str,
                    data) -> bool:
        """dump CSV to file"""
        target = codecs.open(out_file_path, "w", encoding="utf-8")
        target.write(data)
        target.close()

        return True

    @staticmethod
    def json_to_file(out_file_path: str,
                     data,
                     flush_data: bool = True,
                     is_debug: bool = False) -> bool:
        """ dump JSON to file"""

        with io.open(out_file_path, 'w', encoding='utf8') as json_file:
            json_dump = json.dumps(data,
                                   ensure_ascii=False,
                                   indent=4,
                                   sort_keys=True)
            json_file.write(json_dump)

            if flush_data:
                json_file.flush()

        if is_debug:
            print("Wrote to File: {0}".format(out_file_path))

        return True

    @staticmethod
    def load_files(directory: str,
                   extension: str) -> list:
        """loads files in a directory by extension"""
        input_files = []
        for filename in os.listdir(directory):
            if filename.endswith(extension):
                input_files.append(os.path.join(directory, filename))
        return input_files

    @staticmethod
    def create_dir(directory: str) -> bool:
        """ Create the output directory if it doesn't exist """
        if not os.path.exists(directory):
            os.makedirs(directory)
            return True
        return False

    @staticmethod
    def file_exists(file_path: str) -> bool:
        return os.path.exists(file_path) and os.path.isfile(file_path)

    @staticmethod
    def file_to_json(file_path: str,
                     decode_jsonpickle: bool = False):
        with open(file_path) as json_file:
            d = json.load(json_file, encoding="utf-8")
            if decode_jsonpickle:
                return jsonpickle.decode(d)
            return d

    @staticmethod
    def file_to_string(file_path: str) -> str:
        with open(file_path, 'r') as myfile:
            return myfile.read().replace('\n', ' ')

    @staticmethod
    def file_to_lines(file_path: str,
                      use_sort: bool = False) -> list:
        target = codecs.open(file_path,
                             mode="r",
                             encoding="utf-8")
        lines = [x.replace("\n", "").strip() for x in target.readlines() if x]
        target.close()

        if use_sort:
            lines = sorted(lines)

        return lines

    @staticmethod
    def file_to_lines_by_relative_path(relative_file_path: str,
                                       use_sort: bool = False) -> list:
        path = os.path.join(os.environ["CODE_BASE"],
                            relative_file_path)
        return FileIO.file_to_lines(file_path=path,
                                    use_sort=use_sort)

    @staticmethod
    def lines_to_file(some_lines: list,
                      file_path: str) -> None:
        target = codecs.open(file_path,
                             mode="w",
                             encoding="utf-8")
        [target.write("{}\n".format(x)) for x in some_lines if x]
        target.close()

    @staticmethod
    def counter_to_file(c: Counter,
                        file_path: str) -> None:

        target = codecs.open(file_path,
                             mode="w",
                             encoding="utf-8")

        def _record(x):
            return "{}\t{}\n".format(x, c[x])

        [target.write(_record(x)) for x in c]

        target.close()

    @staticmethod
    def dict_to_yaml(d: dict,
                     file_path: str) -> None:
        f = open(file_path, 'w+')
        yaml.dump(d, f, allow_unicode=True)

    @staticmethod
    def file_to_yaml(file_path: str) -> dict:
        with open(file_path, 'r') as stream:
            try:
                return yaml.load(stream, Loader=Loader)
            except yaml.YAMLError:
                raise ValueError("\n".join([
                    "Invalid File",
                    "\t{0}".format(file_path)
                ]))

    @staticmethod
    def file_to_yaml_by_relative_path(relative_file_path: str) -> dict:
        path = os.path.join(os.environ["CODE_BASE"],
                            relative_file_path)
        return FileIO.file_to_yaml(path)

    @staticmethod
    def text_to_file(out_file_path: str,
                     data) -> bool:
        """dump text to file"""
        target = codecs.open(out_file_path, "w", encoding="utf-8")
        target.write(data)
        target.close()

        return True
