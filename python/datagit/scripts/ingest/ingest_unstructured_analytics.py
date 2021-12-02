#!/usr/bin/env python
# -*- coding: UTF-8 -*-


from datagit import CollectionNameGenerator
from datagit.ingest.github.bp import GitHubIngestOrchestrator


def main():
    IS_DEBUG = True
    REPO_OWNER = "GTS-CDO"
    REPO_NAME = "CDO-Workforce-Transformation"
    PERSIST_RECORDS = True

    target_collection_name = CollectionNameGenerator(is_debug=IS_DEBUG,
                                                     repo_name=REPO_NAME,
                                                     collection_type='src').process()

    def ingest(x: int,
               y: int,
               flush_records: bool) -> None:
        GitHubIngestOrchestrator(flush_records=flush_records,
                                 persist_records=PERSIST_RECORDS,
                                 is_debug=IS_DEBUG,
                                 repo_name=REPO_NAME,
                                 repo_owner=REPO_OWNER,
                                 collection_name=target_collection_name).process(start_issue=x,
                                                                                 end_issue=y)

    ingest(0, 1, True)
    ingest(2, 1700, False)


if __name__ == "__main__":
    import plac

    plac.call(main)
