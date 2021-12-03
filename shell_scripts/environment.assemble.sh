#!/usr/bin/env bash

while read -r pkg; do

  [[ -z "$pkg" ]] && continue
  [[ ${pkg:0:1} == '#' ]] && continue

  if ! echo "${PYTHONPATH}" | grep -Eq "$pkg"; then
    PYTHONPATH="${PYTHONPATH:+${PYTHONPATH}:}$CODE_BASE$pkg"
  fi

done <"${CODE_BASE}"/python_envs/assemble-packages.txt

export PYTHONPATH
echo "PYTHONPATH=$PYTHONPATH"

ASSEMBLE_API="${CODE_BASE}/python/dataingest/assemble/bp/assemble_api.py"	
export ASSEMBLE_API
echo "ASSEMBLE_API=$ASSEMBLE_API"