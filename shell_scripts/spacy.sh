#!/usr/bin/env bash

# https://spacy.io/usage/models
python -m spacy download en
python -m spacy download en_core_web_sm
python -m textblob.download_corpora
