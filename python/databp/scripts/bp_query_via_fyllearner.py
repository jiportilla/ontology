#!/usr/bin/env python
# -*- coding: UTF-8 -*-


"""
    Steps:

        1. Given a CSV export of F_YL_LEARNER
        2. Create a unique list of all IBM email addresses
        3. Perform a lookup in the Bluepages API
        4. Populate MongoDB with this information
        5. Then create a DbViz insert file of the Bluepages API information
"""

import codecs

from databp import QueryBluepagesEndpoint


def load_csv_file(f_yl_learner_input):  # STEP 1 and 2
    fo = codecs.open(f_yl_learner_input, "r", encoding="utf-8")
    lines = fo.readlines()
    fo.close()

    lines = [x.split("\t")[3].strip().replace('"', '') for x in lines[1:]]

    print("Total Email Addresses: ", len(lines))
    return lines


def bluepages_lookup(email_addresses):
    total_loaded = QueryBluepagesEndpoint().by_internet_address_bulk(email_addresses)
    print("Total Loaded: ", total_loaded)


def main(f_yl_learner_input):
    email_addresses = load_csv_file(f_yl_learner_input)
    bluepages_lookup(email_addresses)


if __name__ == "__main__":
    import plac

    plac.call(main)
