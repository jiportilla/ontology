#!/usr/bin/env python
# -*- coding: UTF-8 -*-


class RecordUnavailableRecord(Exception):
    def __init__(self, *args, **kwargs):
        Exception.__init__(self, args, kwargs)
