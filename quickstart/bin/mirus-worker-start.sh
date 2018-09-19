#!/usr/bin/env bash
#
# Copyright (c) 2018, salesforce.com, inc.
# All rights reserved.
# SPDX-License-Identifier: BSD-3-Clause
# For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
#
#
# Start individual Mirus worker process
#
# This script follows the general style of kafka-run-class.sh
#
#

readonly base_dir=$(cd $(dirname "$0")/.. && pwd -P)  # Platform independent

MIRUS_MAIN_CLASS=${MIRUS_MAIN_CLASS:-com.salesforce.mirus.Mirus}

CLASSPATH=$(find ${base_dir} -name 'mirus.jar')

# Log directory to use.
if [ -z "${LOG_DIR}" ]; then
    LOG_DIR="${base_dir}/logs"
fi

if [ -z "${LOGGER_OPTS}" ]; then
    export LOGGER_OPTS=" "
fi

# JMX settings.
if [ -z "${MIRUS_JMX_OPTS}" ]; then
  MIRUS_JMX_OPTS=" "
fi


if [ -n "${MIRUS_PORT}" ]; then
  MIRUS_PORT_OPTS="-Dmirus.port=${MIRUS_PORT} "
fi


# Create logs directory.
if [ ! -d "${LOG_DIR}" ]; then
    mkdir -p "${LOG_DIR}"
fi

# Generic jvm settings you want to add.
if [ -z "${MIRUS_OPTS}" ]; then
  # Note: unlike standard Kafka Connect JMX is disabled by default
  MIRUS_OPTS=" "
fi

# Which java to use.
if [ -z "${JAVA_HOME}" ]; then
  JAVA="java"
else
  JAVA="${JAVA_HOME}/bin/java"
fi

# Memory options.
if [ -z "${MIRUS_HEAP_OPTS}" ]; then
  MIRUS_HEAP_OPTS="-Xmx2G"
fi

# JVM performance options.
if [ -z "${MIRUS_JVM_PERFORMANCE_OPTS}" ]; then
  MIRUS_JVM_PERFORMANCE_OPTS="-server -XX:+UseG1GC -XX:MaxGCPauseMillis=20 -XX:InitiatingHeapOccupancyPercent=35 -XX:+DisableExplicitGC -Djava.awt.headless=true"
fi

while [ $# -gt 0 ]; do
  COMMAND=$1
  case ${COMMAND} in
    -loggc)
      if [ -z "${MIRUS_GC_LOG_OPTS}" ]; then
        GC_LOG_ENABLED="true"
      fi
      shift
      ;;
    *)
      break
      ;;
  esac
done

# GC options.
GC_FILE_SUFFIX='-gc.log'
GC_LOG_FILE_NAME=''
if [ "x$GC_LOG_ENABLED" = "xtrue" ]; then
  GC_LOG_FILE_NAME=${DAEMON_NAME}${GC_FILE_SUFFIX}
  MIRUS_GC_LOG_OPTS="-Xloggc:${LOG_DIR}/${GC_LOG_FILE_NAME} -verbose:gc -XX:+PrintGCDetails -XX:+PrintGCDateStamps -XX:+PrintGCTimeStamps "
fi

exec ${JAVA} ${MIRUS_HEAP_OPTS} ${MIRUS_JVM_PERFORMANCE_OPTS} ${MIRUS_GC_LOG_OPTS} ${MIRUS_JMX_OPTS} ${LOGGER_OPTS} \
        -Dmirus.log.dir=${LOG_DIR} ${MIRUS_PORT_OPTS} \
        -cp ${CLASSPATH} ${MIRUS_OPTS} ${MIRUS_MAIN_CLASS} "$@"
