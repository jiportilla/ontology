# !/usr/bin/env python
# -*- coding: UTF-8 -*-


from typing import Optional

from nlusvc import TextAPI
from nlusvc.coords.svc import PerformCoordExtraction

IS_DEBUG = True


def execute_via_svc(input_text: str,
                    entity_text: str,
                    ontology_name: str) -> Optional[dict]:
    svc = PerformCoordExtraction(is_debug=IS_DEBUG,
                                 ontology_name=ontology_name)

    return svc.process(input_text=input_text,
                       entity_text=entity_text)


def execute_via_api(input_text: str,
                    entity_text: str,
                    ontology_name: str) -> Optional[dict]:
    api = TextAPI(is_debug=IS_DEBUG, ontology_name=ontology_name)

    return api.coords(input_text=input_text,
                      entity_text=entity_text)


def compare(input_text: str,
            entity_text: str,
            match_text: str,
            ontology_name: str) -> None:
    """
    Each Test Case is invoked twice
        1. Via the Service directly
        2. Via the API
    """

    # execute by calling the service directly
    svcresult = execute_via_svc(input_text=input_text,
                                entity_text=entity_text,
                                ontology_name=ontology_name)
    assert svcresult is not None
    assert svcresult['substring'] == match_text

    # execute by going through the API
    svcresult = execute_via_api(input_text=input_text,
                                entity_text=entity_text,
                                ontology_name=ontology_name)
    assert svcresult is not None
    assert svcresult['substring'] == match_text


def test_BioTech1():
    compare(entity_text="Natural Killer Cell",  # test exact match and case
            match_text='Natural Killer Cell',
            input_text="Natural Killer Cell",
            ontology_name='biotech')

    compare(entity_text="Natural Killer Cell",  # pre-text, mixed case
            match_text='Natural killer CELL',
            input_text="alpha Natural killer CELL",
            ontology_name='biotech')

    compare(entity_text="Natural Killer Cell",  # pre-and-post text, mixed case
            match_text='natURAL Killer cell',
            input_text="alpha natURAL Killer cell alpha",
            ontology_name='biotech')

    compare(entity_text="Natural Killer Cell",  # multi-text, mixed
            match_text='natural killer (NK) cElls',
            input_text="and the cytotoxic activity of natural killer (NK) cElls and memory CD8+ T cells",
            ontology_name='biotech')

    compare(entity_text="Natural Killer Cell",  # false positives
            match_text='Natural Killer Cell',
            input_text="Natural Killer Natural Killer Cell Cell",
            ontology_name='biotech')

    compare(entity_text="Natural Killer Cell",  # false positives
            match_text='Natural Killer Killer Cell',
            input_text="Natural Natural Natural Killer Killer Natural Killer Killer Cell Cell Cell",
            ontology_name='biotech')


def test_BioTech2():
    compare(entity_text="Memory T Cell",  # test exact match and case
            match_text='Memory T Cell',
            input_text="Memory T Cell",
            ontology_name='biotech')

    compare(entity_text="Memory T Cell",  # pre-text, mixed case
            match_text='mEmoRy t CEll',
            input_text="alpha mEmoRy t CEll",
            ontology_name='biotech')

    compare(entity_text="Memory T Cell",  # pre-and-post text, mixed case
            match_text='memORY t cell',
            input_text="alpha memORY t cell alpha",
            ontology_name='biotech')

    compare(entity_text="Memory T Cell",
            match_text='memory CD8+ T cells',
            input_text="and the cytotoxic activity of natural killer (NK) cells and memory CD8+ T cells",
            ontology_name='biotech')


def test_BioTech3():
    compare(entity_text="White Blood Cell",  # test exact match and case
            match_text='immune cells',
            input_text="in vivo on target immune cells.",
            ontology_name='biotech')


def test_BioTech4():
    compare(entity_text="interleukin 15",  # test exact match and case
            match_text='interleukin (IL)-15',
            input_text="interleukin (IL)-15",
            ontology_name='biotech')

    compare(entity_text="interleukin 15",
            match_text='interleukin (IL)-15',
            input_text="the first interleukin (IL)-15",
            ontology_name='biotech')

    compare(entity_text="interleukin 15",  # pre-and-post text
            match_text='interleukin (IL)-15',
            input_text="the first interleukin (IL)-15 and more text",
            ontology_name='biotech')

    compare(entity_text="interleukin 15",  # crazy punctuation
            match_text='interleukin ((((IL))))-----:::+15',
            input_text="the first interleukin ((((IL))))-----:::+15 and more text",
            ontology_name='biotech')


def test_BioTech5():
    input_text = """and the cytotoxic 
    activity 
    of                          natural killer 
    (NK) cells          and memory 
    CD8+ T cells""".strip()  # lots of new lines and tabs

    compare(entity_text="Cytotoxic Activity",
            match_text="cytotoxic activity",
            input_text=input_text,
            ontology_name='biotech')


def test_Base1():
    input_text = """A blockchain,[1][2][3] originally block chain,[4][5] 
    is a growing list of records, called blocks, 
    that are linked using cryptography.""".strip()

    compare(entity_text="Cryptography",
            match_text='cryptography',
            input_text=input_text,
            ontology_name='base')


def main():
    test_Base1()
    test_BioTech1()
    test_BioTech2()
    test_BioTech3()
    test_BioTech4()
    test_BioTech5()


if __name__ == "__main__":
    main()
