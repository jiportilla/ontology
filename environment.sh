#!/usr/bin/env bash

## Add default usage information
##
##
if [[ "$#" -lt 1 ]]; then
  echo "Usage: $0 <ingest|assemble|parse|xdm>"
else

  # bash/zhs compatible
  THIS_SCRIPT=${BASH_SOURCE[0]:-${(%):-%x}}
  export CODE_BASE="$(cd "$(dirname "${THIS_SCRIPT}")" && pwd)"
  echo "CODE_BASE=$CODE_BASE"

  # Have a shared file where we keep cloud credentials
  TEAM_SECRETS=${CODE_BASE}/shell_scripts/environment.team.sh
  test -f "${TEAM_SECRETS}" && source ${TEAM_SECRETS}
  echo "TEAM_SECRETS=$TEAM_SECRETS"

  # Allow developers to tweak their environment
  PERSONAL_OVERRIDES=${CODE_BASE}/shell_scripts/environment.local.sh
  test -f "${PERSONAL_OVERRIDES}" && source ${PERSONAL_OVERRIDES}
  echo "PERSONAL_OVERRIDES=$PERSONAL_OVERRIDES"

  # Make sure virtualenv is activated
  SITE_PACKAGES="$(python -c 'import site; print(site.getsitepackages()[0])')"
  if [[ ${SITE_PACKAGES} == *"ont"* ]]; then
    echo "  Python virtual environment passed."
    else
    echo "ERROR: You need to use the 'ingest' virtualenv"
    echo "  You can create it with:"
    echo "    conda env create -f=python_envs/ingest.yml"
    echo "  You can activate it with:"
    echo "    conda activate ont"
  fi

  if [[ "$1" == "ingest" ]]; then
    # echo "Processing ingest environment"
    # Ingest ENV
    INGEST_ENV=${CODE_BASE}/shell_scripts/environment.ingest.sh
    test -f "${INGEST_ENV}" && source ${INGEST_ENV}
    echo "INGEST_ENV=$INGEST_ENV"
  fi

  if [[ "$1" == "assemble" ]]; then
    # echo "Processing ingest environment"
    # Ingest ENV
    ASSEMBLE_ENV=${CODE_BASE}/shell_scripts/environment.assemble.sh
    test -f "${ASSEMBLE_ENV}" && source ${ASSEMBLE_ENV}
    echo "ASSEMBLE_ENV=$ASSEMBLE_ENV"
  fi

  if [[ "$1" == "parse" ]]; then
    # echo "Processing ingest environment"
    # Ingest ENV
    PARSE_ENV=${CODE_BASE}/shell_scripts/environment.parse.sh
    test -f "${PARSE_ENV}" && source ${PARSE_ENV}
    echo "PARSE_ENV=$PARSE_ENV"
  fi

  export STOP_WORDS_HF="resources/nlu/other/stopwords_highfreq_kb.csv"
  export STOP_WORDS_LEARNING="resources/nlu/other/stopwords_custom_kb.csv"

fi


# Make sure virtualenv is activated
#SITE_PACKAGES="$(python -c 'import site; print(site.getsitepackages()[0])')"
#if [[ ${SITE_PACKAGES} == *"text"* ]]; then
#  echo ""
#else
#  echo "ERROR: You need to use the text virtualenv"
#  echo "  You can create it with:"
#  echo "    conda env create -f=environment.yml"
#  echo "  You can activate it with:"
#  echo "    conda activate text"
#  return
#fi

# required by the Vector Space API
#mkdir -p "resources/confidential_input/vectorspace"

# required by Testing Services
#mkdir -p "resources/output/testing"

# required by Analysis Services
#mkdir -p "resources/output/analysis"

# required by Source Transformations
#mkdir -p "resources/output/transform"

# required by Grid Script
#mkdir -p "resources/grid/generated"

# Get our project folders in the pythonpath
#while read -r pkg; do

#  [[ -z "$pkg" ]] && continue
#  [[ ${pkg:0:1} == '#' ]] && continue

#  if ! echo "${PYTHONPATH}" | grep -Eq "$pkg"; then
#    PYTHONPATH="${PYTHONPATH:+${PYTHONPATH}:}$CODE_BASE$pkg"
#  fi

#done <"${CODE_BASE}"/ingest-packages.txt
#export PYTHONPATH
#echo "PYTHONPATH=$PYTHONPATH"

#
#export RQ_CHUNK_SIZE_ASSEMBLE=500
#export RQ_CHUNK_SIZE_BADGE=10
#export RQ_CHUNK_SIZE_PARSE=25
#export RQ_CHUNK_SIZE_XDM=50
#export RQ_MAX_WIP_INGEST=20
#export RQ_MAX_WIP_ASSEMBLE=600
#export RQ_TIMEOUT_MINUTES_JOB=60
#export RQ_WORKER_PROCESSES=1

#export TAG_CONFIDENCE_THRESHOLD=50
#export OUTPUT_DIR="${HOME}/Desktop/"
#export INCREMENTAL_ANNOTATION_RETRIEVAL="false"
#export CENDANT_ONTOLOGY="resources/ontology/cendant/cendant.owl"
#export SANDBOX="${CODE_BASE}/python/taskadmin/taskadmin/sandbox"
#export FEEDBACK_API="${CODE_BASE}/python/cendalytics/cendalytics/feedback/consumer/shell/feedback_report_api_caller.py"

#export STOP_WORDS_HF="resources/nlu/other/stopwords_highfreq_kb.csv"
#export STOP_WORDS_LEARNING="resources/nlu/other/stopwords_custom_kb.csv"

#export BOX_API_CONFIG="${CODE_BASE}/resources/box-api/455328_3nnayk1b_config.json"
#export ASSEMBLE_API_BULK_INSERT_THRESHOLD=1000

#export REDIS_HOST=${REDIS_HOST:-localhost}
#export REDIS_PORT=${REDIS_PORT:-6379}

# MONGO_JSON_CREDENTIALS
#export MONGO_JSON_CREDENTIALS=${MONGO_JSON_CREDENTIALS:-\{\"warning\": \"set the value in environment.local.sh\"\}}
#export MONGO_HOST=${MONGO_HOST:-wftag}
#export MONGO_PORT=${MONGO_PORT:-27017}
#export MONGO_USER_NAME=${MONGO_USER_NAME:-gAAAAABc-BjhZDS9KzN91syIrbVWSB26NhsXZ4vaIPaGwB4acbJHX2usdRqbgKlSc8RmntVyYPas5Py98etBpO8aahlYXCHAVA==}
#export MONGO_PASS_WORD=${MONGO_PASS_WORD:-gAAAAABc-VMR015hA9Xtlo7a5ZaP6tGOaL1hCJ2fL-uOzkUAX7uWXDm0UkvjGOZY81HpNRP-RFF66haqGXQ_2Bi_YWEHYwPpig==}

#export DB2_USER_NAME="gAAAAABc_9wXRU7oAGxwQwTVt3riX1FOTNkUVNDPYEQEdQd-MwgyQyPzI34vRIqhlOTYDCowjZnqBPhVp_GucTyf0yKvH-1m1Q=="
#export DB2_PASS_WORD="gAAAAABc_9v33C7OHl3O5jz2az7y1SIKyJmQMjt0birhazoTo81_iNWEJU83i7II7luozzAJ2g2XvCxD8bMpdL4vd-4Y3Fi8oA=="

#export LUR_USER_NAME="gAAAAABdQz2UPV-2jzzPo8rQqX3zTlaeVB6E0hk7XjT_-lniiwSqIcedLhuLvHJXJBxo-M0dD0RKCJr_abNghEooNnOqwd0OHg=="
#export LUR_PASS_WORD="gAAAAABf_22fKsH8wqzx3oJjvN_cAeh9Wsz93yCS_Xhwm-Uot_LweSkppn4D8_1pdGhodseOjTgkuTqlg4gc9x6y9XXk3RCQEw=="

#export WFT_USER_NAME="gAAAAABgFDX3OeKpZrG7oOxVcsWgQLJO0c608Hk6oBpKPQR3w5RifiA2Nqn_OOMmnBJ4mX_NVg9FdUF76iLDImMJ-vfYEmQxWQ=="
#export WFT_PASS_WORD="gAAAAABgFDYn0D3rDFqGTWsuDCR1D-f_YCGyK-ecRRcjq36QXW_v9gSUhpgdMQ5lWJcGzGdUjhwgm4ksCsAhiRJ6Ui-IWgh3MvTvuhTBCGtrbK_uk2dtf1E="

#export CENDANT_USER_NAME="gAAAAABdnm0Ms2cXOzGWM5yIbONLa7IwfYpqTczSR9CZz5yfTe7dNKmPn9PpvAw_Rwu0BWwtdu6yWTBnsh6ybSvcreHzxyl5Qg=="
#export CENDANT_PASS_WORD="gAAAAABdnm0gG85qaTFLff4mhHGUvbLUZaRuMH6qQlgY3gJcJIkdvzkfo9aG7HC0-usC3_gbPDAihcmr3SdLx_ihI4txQIkBvA=="

#export GITHUB_USER_NAME="gAAAAABd3bVPMn_cmkBkoPiNGi253lGfh_UqHNNTvsSPH0__asuIj4T9x23QYrM3dQis5K30fuSBKqR4ZMsPvRekPAdqu0TKO76O_sE-qu4hpP0Nc7cEmA8="
#export GITHUB_BASE_URL="gAAAAABd3gaRMNjvrGBF_rJ1Xl36T1EtA8lZ1xqmeGubQJZ845uzuuw1PcHsp-QIZ6SSxAt_4XL6Ls4axjQyeQklUT6k5-fRswZUqyDMLugC0iUz1_hAHx_L-6Qu74qogPYlqaC-PkQd"

# github is rate limited; so it helps to have multiple user tokens and swap them
#export GITHUB_USER_TOKEN_1="gAAAAABd-wOa3CVoEbviq_HguAHBicYrrBsDtNKYj1J1HdqpV59fXvg763-3JKZ6A2YzaS40Cw1xDvNP1ZsbnCcU1GjHBtxclGM8U6EHbWLWASWj72tQX68soUIcx5Jy7pqgkYxkCL5E"
#export GITHUB_USER_TOKEN_2="gAAAAABd-wOg7WhBOmHgO5IC42PKlkouYdw-akb3qb65qu1py2D0GZDcgegez4CryK8_kOGFn8zE8RaifIYGz5jv-CpG0qacWXnUY78NZczRqfhnLmgwwqNS9wTjs5rkEra_axtZLpqi"
#export GITHUB_USER_TOKEN_3="gAAAAABd-wOkz3cxAJnyWEjUXrBZI7yFgE0uHW0LvZEYdsXNwVoXsbbDeueha5uRsPwsGHfhIWs7cB8CZRLjr5P6dqp96e2aRQDTFZSV8R_RPY3Tt70bT_QogVjVnQxRZT2kDHufvQwb"
#export GITHUB_USER_TOKEN_4="gAAAAABd-wOnJ-XAMnF6-AuLPSRSfnsMR8bE6l5c3xuMuT008kXgFJXYV7ArpJgaA4UdX1Ivza1g1Or71tr4PpV59TKlgu0nPc9Ngtepfk27ciFgsK-R7P4AzjNnniSmlrEI9ezCXDHz"
#export GITHUB_USER_TOKEN_5="gAAAAABd-wOr4d6c_1b4M5o-4FWrAO4dOVyCdI5sBqxVO0-KimPrD5DCTaSs87OI9zO-o3aLMY2In60Ru7Ms1-QwTvPwxNYzFvb97Ubfn7AfGly7fQRgs2Znr1k1gXfPhZyCkJ40MUtF"
#export GITHUB_USER_TOKEN_6="gAAAAABd-wO0337u0NFVoytLeEHoHeci-WmQ59KGoQt0agyDC-NzcfjkHrJyqgIG85ecNpSr6yGgJeQQQGBv-qZi0w8EXtiArF3On2pr8MSSGNSbuvtG4SusnEeM9xfkXem-GdGwR2o2"
#export GITHUB_USER_TOKEN_7="gAAAAABd-wem-snadRm48fprNqQB79FU7vHfQvkxSIBHaBpt2xLC6yFsKbaVLVv_3P4U3t_LKaWHg2ZaBB_QI7YXYXmnPqYDXuFA2jvSNxnXIQMOxixDZsXZZQnqD9BA8Ns5orZIwH0V"
#export GITHUB_USER_TOKEN_8="gAAAAABd-weqb_dawi1BZx0b9A6vI23qo_rQqlBs1an_vrjLiiwoVFv4VDx6xy4h2YyVNETSZojIRQ8ejyR7WYPQcVL9fJi2FFcwkUeOlKpTDwF2xXz2pZnygdDclb0KwH7EHeYn5gx9"
#export GITHUB_USER_TOKEN_9="gAAAAABd-wetBsPGlMKmw0Drk3aiUKjUcjDTjk4mOA-ENDIJYjR_Gmnkv3nl41IM4Z0ezLxDBBr8TxSWa5WfU_58zxBhcUaIu9tb9Q2GoyNdcmeO9d85YiBhFwKCNNlUkExPs-fCoGSc"
#export GITHUB_USER_TOKEN_10="gAAAAABd-xPOL_WV2qTxiZEJVM3P910vRIpP6eDZc1nxUSWjfkvGxATmNMLlSuFEpgs3E4c9f79AlbDchk50fH8j_oxZIdfblATpcr9mBY9BSNBZTnbkqdAHPSTzUfG9Yg6gJvuZXiX8"
#export GITHUB_USER_TOKEN_11="gAAAAABd-xPSyBIeJaNkEnRWbsLbTajdGOq3FHTIGij_YK-g2TAYhmsQSkPJZjmoTOW__S0B3KMkveyA4bFATDD0Wl6vpSMETG2AJLrjZjDqsc9u3zcVhpk64jpVqsTwdkXYkm5e-Wl_"
#export GITHUB_USER_TOKEN_12="gAAAAABd-xPXLC7p_2QoMIzaBFcCrn_fupE5CHzN9Rc0DFfsJC6TuY496qhoSpjOAsk8s40zt-yU7nf4Joz4gJ5CiHTXYgliqZ0v-Pebr6YkCbT3p72IRLnqEGv4s58XZzG4X3MnwmpI"

#export ZENHUB_BASE_URL="gAAAAABd7FEyohcEX4vAtRUY2KVsBu11RLJBCf8TOAgR5hys_SvDzbg7m6OH8JwZgUHN-77IE1Ap3Q58COGZsa3uZvzjaxX8lg7IwHNAoHF0hJd-CY7pzmphECJV7rmaCV4eeeC2_pOEAWUa_heoheiyum65XCNz0A=="
#export ZENHUB_USER_TOKEN="gAAAAABd7FDlqZb8vRXtANKI3AocMg7lojd0hfWH3AlnmXYFGJJKwvZKpsGTegfI63DYOn_lUuFdxEsUgnT6Ved-ebTfLWo_HHCFU7C9QyyWnVts8ZR3k7bkqidyxcDkocVNQIyQZ2jEZRrKd-NU3MPWNu1d4EbT7LDMRJZ-pt2EAea5YRu4uFs1JjDgcw9dpRigaT6b5a17"

#export SLACK_INGEST_API="gAAAAABeXRiYk6rdSkfMJiDrIXFs8TrdGFuJ9NgV4p69I8p2D1pbe9lcZFODo8C08WEp7o2-hbdCEF_4VQ73-KphIPClS0Q2ZdW3KAdCNEwXClmI4oHOmqhuYOk51pMG0p0CNoTbcYEXsWIIyEqsSRLYaGm0td0xaEj_aYolWf_4BIqD8kWhVF4="

# output directory
#mkdir -p resources/output

# Expand the variable named by $1 into its value. Works in both {ba,z}sh
# eg: a=HOME $(var_expand $a) == /home/me
# https://unix.stackexchange.com/a/472069/91054
#var_expand() {
#  if [ -z "${1-}" ] || [ $# -ne 1 ]; then
#    printf 'var_expand: expected one argument\n' >&2;
#    exit 1;
#  fi
#  eval printf '%s' "\"\${$1?}\""
#}

# Generate a .env file with the vars that be have set here.
# VS Code takes advantage of this file
#echo "# Automatically generated" >"${CODE_BASE}"/.env
#grep '^export' <"${THIS_SCRIPT}" | cut -d " " -f 2 | cut -d "=" -f 1 | sort | while read -r line; do
#  echo "${line}"=\"$(var_expand $line)\" >>"${CODE_BASE}"/.env
#done

# On OSX, make sure that we bypass the pip installation error
# in ibm_db that prevents its proper loading
#if [[ $(uname -s) == Darwin ]] ; then
#  ibm_db_lib=$(echo ${SITE_PACKAGES}/ibm_db.*.so)
#  load_path=@loader_path/clidriver/lib/libdb2.dylib
#  if otool -l ${ibm_db_lib} | grep -q ${load_path} ; then
#    echo 'No need to update ibm_db.*.so'
#  else
#    install_name_tool -change libdb2.dylib ${load_path} ${ibm_db_lib}
#  fi
#fi

# When running on Kubernetes, copy the db2 license where it belongs
#if [[ ! -z "${KUBERNETES_SERVICE_HOST}" ]]; then
#  # echo "Running in kubernetes"
#  if [[ ! -d /home/db2_cli_odbc_driver/clidriver/license-tmp/ ]]; then
#    echo "WARNING: missing volume with DB2 license"
#  else
#    cp /home/db2_cli_odbc_driver/clidriver/license-tmp/* /home/db2_cli_odbc_driver/clidriver/license/
    # ls -latr /home/db2_cli_odbc_driver/clidriver/license/
#  fi
#fi

#alias lint="pylint --list-msgs | grep"
#alias prodigy="python -m prodigy"
