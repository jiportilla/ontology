# !/usr/bin/env python
# -*- coding: UTF-8 -*-


import pandas as pd
from tabulate import tabulate

from base import FileIO
from datagraph import GraphvizAPI

IS_DEBUG = True


def main():
    api = GraphvizAPI(is_debug=IS_DEBUG)

    rels = []

    df = pd.read_csv("/Users/craig.trimibm.com/Desktop/emp-results.csv")
    print(tabulate(df.sample(5), headers='keys', tablefmt='psql'))

    for cnum in df['CNUM'].unique():
        df2 = df[df['CNUM'] == cnum]
        terms = df2['Term'].unique()

        for term1 in terms:
            for term2 in terms:
                rels.append(' -> '.join(sorted({term1, term2})))


    # for _, row in df.iterrows():
    #     cnum = row['CNUM']
    #     freq = int(row['Freq'])
    #     term = row['Term']
    #
    #     if term == "facility management":
    #         continue
    #
    #     for i in range(0, freq):
    #         rels.append(f"{cnum} -> {term}")

    output = api.generate_file(rels=rels)
    FileIO.lines_to_file(output, "/Users/craig.trimibm.com/Desktop/graph-skills.giz")


if __name__ == "__main__":
    main()
