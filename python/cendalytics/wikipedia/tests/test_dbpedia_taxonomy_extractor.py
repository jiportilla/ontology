# !/usr/bin/env python
# -*- coding: UTF-8 -*-


from typing import Optional

from cendalytics.wikipedia.ingest.dmo import DBpediaTaxonomyExtractor

IS_DEBUG = True


def execute(input_text: str) -> Optional[str]:
    subclass = DBpediaTaxonomyExtractor(is_debug=IS_DEBUG,
                                        input_text=input_text).process()
    if subclass:
        return subclass
    return "NULL"


def compare(input_text: str,
            expected_output_text: str) -> bool:
    actual_output_text = execute(input_text)
    print (f"Output Text - A: {actual_output_text}")
    print (f"Output Text - E: {expected_output_text}")
    return actual_output_text.lower() == expected_output_text.lower()


def test_taxonomy_extractor():
    assert compare(expected_output_text="malignant lung tumor",
                   input_text="Lung cancer is a malignant lung tumor characterized by uncontrolled cell growth.") \
           is True

    assert compare(expected_output_text="malignant lung tumor",
                   input_text="Lung cancer, also known as lung carcinoma, is a malignant lung tumor characterized by.") \
           is True

    assert compare(expected_output_text="RNA-like molecule",
                   input_text="An L-ribonucleic acid aptamer is an RNA-like molecule built from L-ribose units.") \
           is True

    assert compare(expected_output_text="molecule",
                   input_text="""A polymer (; Greek poly-, "many" + -mer, "part") is a large molecule, or macromolecule.""") \
           is True

    assert compare(expected_output_text="biopharmaceutical company",
                   input_text="""Innate Immunotherapeutics is a biopharmaceutical company, formerly known as Virionyx""") \
           is True

    assert compare(expected_output_text="white blood cell",
                   input_text="""A lymphocyte is one of the subtypes of a white blood cell in a vertebrate's immune system.""") \
           is True


def main():
    test_taxonomy_extractor()


if __name__ == "__main__":
    main()
