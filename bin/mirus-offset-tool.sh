#!/bin/bash
#
# Copyright (c) 2018, salesforce.com, inc.
# All rights reserved.
# SPDX-License-Identifier: BSD-3-Clause
# For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
#
#
# Mirus Offset Tool
#
# A simple tool for reading and writing Mirus offsets
#

readonly base_dir=$(cd $(dirname "$0")/.. && pwd -P)

MIRUS_OFFSET_TOOL_MAIN_CLASS=${MIRUS_OFFSET_TOOL_MAIN_CLASS:-com.salesforce.mirus.offsets.MirusOffsetTool}

# Which java to use
if [ -z "${JAVA_HOME}" ]; then
  JAVA="java"
else
  JAVA="${JAVA_HOME}/bin/java"
fi

${JAVA} ${MIRUS_OPTS} -cp "${base_dir}/mirus.jar" "${MIRUS_OFFSET_TOOL_MAIN_CLASS}" "$@"
