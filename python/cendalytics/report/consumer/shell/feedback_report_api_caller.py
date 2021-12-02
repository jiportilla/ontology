#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Created:
    13-Nov-2019
    craig.trim@ibm.com
    *   usage referenc
        https://github.ibm.com/GTS-CDO/unstructured-analytics/issues/1351
"""

import click

from cendalytics.report.core.bp import ReportAPI

IS_DEBUG = True


def init_api() -> ReportAPI:
    from datamongo import BaseMongoClient
    return ReportAPI(is_debug=IS_DEBUG,
                     mongo_client=BaseMongoClient())


# Command Group: Learning Reports
@click.group()
def cli1():
    # ensure that ctx.obj exists and is a dict (in case `cli()` is called
    # by means other than the `if` block below
    pass


@cli1.command()
@click.option('--id', help='Learning ID', default="random",
              prompt='Please enter the Learning ID (LAID)')
@click.option('--collection', help='Collection Date',
              default="latest")
def supply(id, collection):
    """Learning Report by Serial Number"""
    init_api().report().by_learning_id(learning_id=id,
                                       collection_date=collection).process()


# Command Group: Supply Reports
@click.group()
def cli2():
    # ensure that ctx.obj exists and is a dict (in case `cli()` is called
    # by means other than the `if` block below
    pass


@cli2.command()
@click.option('--id', help='Serial Number', default="random",
              prompt='Please enter the Serial Number')
@click.option('--collection', help='Collection Date',
              default="latest")
def supply(id, collection):
    """Supply Report by Serial Number"""
    init_api().report().by_serial_number(serial_number=id,
                                         collection_date=collection).process()


# Command Group: Demand Reports
@click.group()
def cli3():
    # ensure that ctx.obj exists and is a dict (in case `cli()` is called
    # by means other than the `if` block below
    pass


@cli3.command()
@click.option('--id', help='OpenSeat ID', default="random",
              prompt='Please enter the OpenSeat ID')
@click.option('--collection', help='Collection Date',
              default="latest")
def demand(id, collection):
    """Demand Report by OpenSeat ID"""
    init_api().report().by_openseat_id(openseat_id=id,
                                       collection_date=collection).process()


cli = click.CommandCollection(sources=[cli2])

if __name__ == '__main__':
    cli()
