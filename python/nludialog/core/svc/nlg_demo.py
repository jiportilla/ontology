#!/usr/bin/env python
# -*- coding: UTF-8 -*-


from nlusvc import TextAPI

text_api = TextAPI(is_debug=True)

if __name__ == "__main__":
    input_text = """
    Using cognos TM1's unique features
    """.strip()

    svcresult = text_api.parse(input_text)

    print (svcresult)
