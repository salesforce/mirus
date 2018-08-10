#!/usr/bin/env bash
#
# Copyright (c) 2018, salesforce.com, inc.
# All rights reserved.
# SPDX-License-Identifier: BSD-3-Clause
# For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
#
# Start a set of Mirus worker processes. All worker processes share the same destination
# cluster and are part of the same Kafka Connect logical cluster.  The script takes one or more
# port numbers and a worker is started on each port.
#
# Environment variables:
#
# PID_LOCATION=directory for process control PID files
#

set -euf -o pipefail

readonly base_dir=$(cd $(dirname "$0")/.. && pwd -P)
readonly PID_LOCATION=${PID_LOCATION:-"${base_dir}/pid"}
readonly start_timeout=${START_TIMEOUT:-120}

wait_startup() {
    local port=$1

    loop_time=0
    until $(nc -w 1 localhost "${port}" > /dev/null); do
        sleep 1

        if  [ ${loop_time} -ge ${start_timeout} ]
        then
            echo "Timeout waiting for startup"
            exit 1
        fi

        let "loop_time=loop_time+1"
    done
}

store_pid(){
    local pid=$1
    local port=$2
    local dest_dc=$3
    pid_file="${PID_LOCATION}/mirus_${dest_dc}_${port}_${pid}.pid"
    if [ ! -f "${pid_file}" ]; then
        touch "${pid_file}"
        echo "${pid}" >> "${pid_file}"
    fi
}

start_worker() {
    local port=$1
    # Export MIRUS_PORT env var: is used for log filename
    export MIRUS_PORT=${port}
    echo "Starting Mirus worker process (cluster: \"${cluster}\", port: $port) ..."
    nohup -- ${base_dir}/bin/mirus-worker-start.sh \
        "${worker_properties}" --override "rest.port=${port}" >> ${LOG_DIR}/mirus_${cluster}_${port}.out 2>&1 &
    store_pid $! ${port} ${cluster}
}

usage (){
    local usage="Usage: $(basename "$0") -f <filename>  -c <label> -p <port>
Start one Mirus worker process on each specified port.

Options:
    -f: Mirus worker instance properties filename
    -c: Cluster label (short name for the destination cluster)
    -p: Admin port (supports multiple ports, one process per port, config submitted to first port).
    -h: Show help
"
    echo "$usage" >&2
}

while getopts 'f:p:c:h' option; do
    case ${option} in
    f)  readonly worker_properties="${OPTARG}"
        ;;
    p)  ports+=("${OPTARG}")
        ;;
    c)  readonly cluster=${OPTARG}
        ;;
    h)  usage
        exit
        ;;
    :)  printf "Missing argument for -%s\n" "${OPTARG}" >&2
        usage
        exit 1
        ;;
    \?) printf "Illegal option: -%s\n" "${OPTARG}" >&2
        usage
        exit 1
        ;;
    *)  echo "Unimplemented option: -${OPTARG}" >&2;
        exit 1
        ;;
  esac
done

if [ ${OPTIND} -eq 1 ];
then
    # No options
    usage
    exit 1
fi

# Ensure PID directory exists
mkdir -p "${PID_LOCATION}"

echo "Waiting for all workers to start ..."

# Start one worker process for each port (in parallel to minimize re-balances)
for port in "${ports[@]}"; do
    start_worker ${port} &
done

# Now wait for all of these to complete
for port in "${ports[@]}"; do
    wait_startup ${port}
    echo "Worker started (cluster: \"${cluster}\", port: $port)"
done

echo "Mirus started successfully"
