# !/usr/bin/env python  # -*- coding: UTF-8 -*-

import logging


"""
    Updated:
        28-Oct-2016
        craig.trim@ibm.com
        *   removed "im" from "i am"
            defect 270 "im" => Citi IM
    Updated:
        2-Nov-2016
        craig.trim@.ibm.com
        *   added ["what is", "here is", "that is"]
    Updated:
        4-Nov-2016
        craig.trim@.ibm.com
        *   added ["it is"]
    Updated:
        25-Feb-2017
        craig.trim@.ibm.com
        *   added ["how is"]
    Updated:
        17-Mar-2017
        craig.trim@.ibm.com
        *   added ["i have"]
    Updated:
        20-May-2017
        craig.trim@ibm.com
        *   adeed ["how are"]
"""

the_enclitics_dict = {
    u"i am": [u"i{0}m"],
    u"i have": [u"i{0}ve"],
    u"we have": [u"we{0}ve"],
    u"they have": [u"they{0}ve"],
    u"i would": [u"i{0}d"],
    u"it is": [u"it{0}s"],
    u"can not": [u"can{0}t", u"can not", u"cant", u"cannot"],
    u"is not": [u"isn{0}t", u"isnt"],
    u"do not": [u"don{0}t", u"dont"],
    u"does not": [u"doesn{0}t", u"doesnt"],
    u"will not": [u"won{0}t", u"wont"],
    u"should not": [u"shouldn{0}t", u"shouldnt"],
    u"could not": [u"couldn{0}t", u"couldnt"],
    u"has not": [u"hasn{0}t", u"hasnt"],
    u"what is": [u"what{0}s", u"whats"],
    u"here is": [u"here{0}s", u"heres"],
    u"that is": [u"that{0}s", u"thats"],
    u"how is": [u"how{0}s", u"hows"],
    u"how are": [u"how{0}re"]
}
