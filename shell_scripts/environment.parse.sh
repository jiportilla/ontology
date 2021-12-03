#!/usr/bin/env bash

while read -r pkg; do

  [[ -z "$pkg" ]] && continue
  [[ ${pkg:0:1} == '#' ]] && continue

  if ! echo "${PYTHONPATH}" | grep -Eq "$pkg"; then
    PYTHONPATH="${PYTHONPATH:+${PYTHONPATH}:}$CODE_BASE$pkg"
  fi

done <"${CODE_BASE}"/python_envs/parse-packages.txt

export PYTHONPATH
echo "PYTHONPATH=$PYTHONPATH"

PARSE_API="${CODE_BASE}/python/dataingest/parse/bp/parse_api.py"
export PARSE_API
echo "PARSE_API=$PARSE_API"
export TAG_CONFIDENCE_THRESHOLD=50


