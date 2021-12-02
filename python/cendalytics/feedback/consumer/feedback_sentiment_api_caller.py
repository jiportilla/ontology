#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Created:
    25-Nov-2019
    craig.trim@ibm.com
    *   usage reference
        https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1439
"""

import sys

import click

from cendalytics.feedback.core.bp import FeedbackSentimentAPI


def init_api(is_debug: bool) -> FeedbackSentimentAPI:
    return FeedbackSentimentAPI(is_debug=is_debug)


# Command Group: Learning Reports
@click.group()
def cli1():
    # ensure that ctx.obj exists and is a dict (in case `cli()` is called
    # by means other than the `if` block below
    pass


@cli1.command()
@click.option('--total_records', help='Total Records', default=sys.maxsize)
@click.option('--collection', help='Collection Date',
              default="feedback_tag_20191202")
def report(total_records, collection):
    """Sentiment Report by Collection"""

    def is_debug() -> bool:
        return total_records and total_records <= 500

    init_api(is_debug=is_debug()).generate(collection_name=collection).reports().process(total_records=total_records)


cli = click.CommandCollection(sources=[cli1])

if __name__ == '__main__':
    cli()
