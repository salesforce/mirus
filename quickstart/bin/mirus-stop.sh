#!/usr/bin/env bash
#
# Copyright (c) 2018, salesforce.com, inc.
# All rights reserved.
# SPDX-License-Identifier: BSD-3-Clause
# For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
#
#
# Stop all Mirus worker processes with pid files.
#

set -eu -o pipefail

readonly base_dir=$(cd $(dirname "$0")/.. && pwd -P)
readonly pid_location=${PID_LOCATION:-"${base_dir}/pid"}
readonly timeout_seconds=${TIMEOUT_SECONDS:-120}

current_timeout=${timeout_seconds}

stop_all(){

    local pid_files=${pid_location}/mirus*.pid

    # Submit stop signal to all workers
    for pid_file in ${pid_files} ; do
        if [ -e "${pid_file}" ]; then
            submit_signal "${pid_file}" "SIGTERM"
        fi
    done

    # Wait for all to stop
    local fail=0
    for pid_file in ${pid_files} ; do
        if [ -e "${pid_file}" ]; then
            wait_stop_pid "${pid_file}"
            status=$?
            fail=$((fail+status))
        fi
    done

    if (( ${fail} == 0 )); then
        echo "SUCCESS: Stopped all Mirus workers"
        exit 0
    else
        echo "FAILURE: Could not stop all workers within ${timeout_seconds} seconds. Sending SIGKILL."
            # Submit stop signal to all workers
        for pid_file in ${pid_files} ; do
            if [ -e "${pid_file}" ]; then
                submit_signal "${pid_file}" "SIGKILL"
            fi
        done
        exit 1
    fi
}

submit_signal() {
    local pid_file=$1
    local signal=$2

    local pid=$(cat ${pid_file})

    echo "Sending ${signal} to pid ${pid}"
    /bin/kill -s "${signal}" "${pid}" || true
}


wait_stop_pid(){
    local pid_file=$1
    local pid=$(cat ${pid_file})
    while [ ${current_timeout} -gt 1 ]
    do
        exists=$(ps -o pid= -p ${pid}) || true
        if [[ -z "${exists}" ]]; then
            echo "Process has stopped. Removing PID file: ${pid_file}"
            rm "${pid_file}"
            return 0
        fi
        sleep 1
        ((current_timeout--))
    done
    return 1
}

stop_all
