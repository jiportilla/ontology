#!/usr/bin/env python
# -*- coding: UTF-8 -*-


import time


class GraphAPI:
    """ API for Graph functionality

        Created:
            25-Feb-2019
            craig.trim@ibm.com
            *   refactored from various services """

    @classmethod
    def initialize_neo_graph(cls):
        from datagraph import OwlGraphConnector
        from datagraph import InitializeNeoGraph

        start = time.time()

        owlg = OwlGraphConnector("cendant").process()

        print("\n".join([
            "Loaded Graphs: {}".format(
                time.time() - start)]))

        InitializeNeoGraph(some_owl_graph=owlg).process()


def main(param1):
    if param1 == "init":
        GraphAPI.initialize_neo_graph()


if __name__ == "__main__":
    import plac

    plac.call(main)
