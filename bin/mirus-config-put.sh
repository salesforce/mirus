#!/usr/bin/env bash
#
# Copyright (c) 2018, salesforce.com, inc.
# All rights reserved.
# SPDX-License-Identifier: BSD-3-Clause
# For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
#
#
# Submit a Mirus source config file to the Kafka Connect config api
#
#
set -euf

usage() {
    local usage="Usage: $(basename "$0") <source connector json config file> <port>"
    echo "${usage}" >&2
}

if [ $# -ne 2 ]
then
    usage
    exit 1
fi

readonly source_connector_config_file=$1
readonly port=$2
readonly api_uri="localhost:${port}"
readonly curl_timeout=${CURL_TIMEOUT:-10}

config_file_content=$(cat "${source_connector_config_file}")

# Get name from config file
connector_name=$(echo "${config_file_content}" | python -c 'import sys, json; print(json.load(sys.stdin)["name"])')

exit_code=0
curl "${api_uri}/connectors/${connector_name}/config" \
    --silent \
    --fail \
    --output /dev/null \
    -H 'Content-Type: application/json' \
    -X PUT \
    --max-time "${curl_timeout}" \
    --data "${config_file_content}" || exit_code=$?

if [ "${exit_code}" != "0" ] ; then
    echo "Config PUT failed with curl exit code ${exit_code}: ${api_uri}/connectors/${connector_name}/config"
    exit 1
fi
