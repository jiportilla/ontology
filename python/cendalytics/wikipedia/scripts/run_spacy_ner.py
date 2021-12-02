#!/usr/bin/env python
# -*- coding: UTF-8 -*-

import spacy

IS_DEBUG = True
IGNORE_CACHE = True
ONTOLOGY_NAME = "biotech"


def main():
    nlp = spacy.load("en_core_web_sm")
    # doc = nlp("Apple is looking at buying U.K. startup for $1 billion")

    # for token in doc:
    #     print(token.text, token.lemma_, token.pos_, token.tag_, token.dep_,
    #           token.shape_, token.is_alpha, token.is_stop)

    input_text = """
    The investment arm of biotech giant Amgen has led a new $6 million round of funding in GNS Healthcare, CEO Colin Hill tells Xconomy.
    """.strip()

    doc = nlp(input_text)
    for chunk in doc.noun_chunks:
        print(chunk.text, chunk.root.text, chunk.root.dep_,
              chunk.root.head.text)

    print ('------------------------------------------')
    for ent in doc.ents:
        print(ent.text, ent.start_char, ent.end_char, ent.label_)

if __name__ == "__main__":
    import plac

    plac.call(main)
