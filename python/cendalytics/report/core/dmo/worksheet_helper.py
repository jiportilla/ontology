#!/usr/bin/env python
# -*- coding: utf-8 -*-




import string
from openpyxl.worksheet.worksheet import Worksheet


class WorksheetHelper:

    @classmethod
    def column_letters(cls) -> list:
        """
        Purpose:
            Generate all possible excel column letters
        Sample Output:
            [   'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M',
                'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z',
                'AA', 'AB', 'AC',   ...    'ZX', 'ZY', 'ZZ']
        :return:
            list of excel column letters
        """
        alphabet = [x for x in string.ascii_uppercase]
        for ch in string.ascii_uppercase:
            [alphabet.append(alpha) for alpha in [f"{ch}{x}" for x in string.ascii_uppercase]]

        return alphabet


    @classmethod
    def column_widths(cls,
                      worksheet: Worksheet,
                      d_columns: dict) -> None:
        """
        :param worksheet:
            an active Excel worksheet
        :param d_columns:
            a dictionary of columns and widths
            e.g.    {   'A': 10,
                        'B': 10 }
        """

        def _update(alpha: str):
            worksheet.column_dimensions[alpha].width = d_columns[alpha]

        [_update(alpha) for alpha in d_columns]

    @classmethod
    def struct(cls,
               value: str,
               style: str) -> dict:
        return {"value": value,
                "style": style}

    @classmethod
    def generate(cls,
                 worksheet: Worksheet,
                 l_structs: list) -> None:
        """
        :param worksheet:
            an active Excel worksheet
        :param l_structs:
            a list of structures
            e.g.    [   {   'value': 'Some Text Value',
                            'style': 'predefined-style' },
                        ...
                        {   'value': 'Another Text Value',
                            'style': 'some-other-style' } ]
        """
        for d in l_structs:
            for k in d:
                worksheet[k].value = d[k]["value"]
                worksheet[k].style = d[k]["style"]
