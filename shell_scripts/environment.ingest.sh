#!/usr/bin/env bash

while read -r pkg; do

  [[ -z "$pkg" ]] && continue
  [[ ${pkg:0:1} == '#' ]] && continue

  if ! echo "${PYTHONPATH}" | grep -Eq "$pkg"; then
    PYTHONPATH="${PYTHONPATH:+${PYTHONPATH}:}$CODE_BASE$pkg"
  fi

done <"${CODE_BASE}"/python_envs/ingest-packages.txt

export PYTHONPATH
echo "PYTHONPATH=$PYTHONPATH"

INGEST_API="${CODE_BASE}/python/dataingest/core/bp/ingest_api.py"
export INGEST_API
echo "INGEST_API=$INGEST_API"