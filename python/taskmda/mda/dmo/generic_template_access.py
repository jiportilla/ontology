#!/usr/bin/env python
# -*- coding: UTF-8 -*-


class GenericTemplateAccess(object):
    """
    Created:
        28-Jul-2017
        craig.trim@ibm.com
    """

    @staticmethod
    def process():
        from taskmda.mda.dto import the_generic_template

        template = the_generic_template

        return template
