/*
 *  Copyright (c) 2018, salesforce.com, inc.
 *  All rights reserved.
 *  SPDX-License-Identifier: BSD-3-Clause
 *  For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 *
 */

package com.salesforce.mirus.config;

import java.util.Arrays;
import java.util.List;
import org.apache.kafka.common.config.ConfigDef;

public class TaskConfigDefinition {

  public static final String PARTITION_LIST = "partitions";
  public static final String CONSUMER_CLIENT_ID = "consumer.client.id";
  /** List of config definitions to inherit from SourceConfig */
  private static final List<SourceConfigDefinition> SOURCE_CONFIG_DEFINITION_LIST =
      Arrays.asList(
          SourceConfigDefinition.POLL_TIMEOUT_MS,
          SourceConfigDefinition.DESTINATION_TOPIC_NAME_PREFIX,
          SourceConfigDefinition.DESTINATION_TOPIC_NAME_SUFFIX,
          SourceConfigDefinition.ENABLE_PARTITION_MATCHING,
          SourceConfigDefinition.SOURCE_KEY_CONVERTER,
          SourceConfigDefinition.SOURCE_VALUE_CONVERTER,
          SourceConfigDefinition.SOURCE_HEADER_CONVERTER);

  static ConfigDef configDef() {
    ConfigDef configDef = new ConfigDef();
    SOURCE_CONFIG_DEFINITION_LIST.forEach(
        f -> configDef.define(f.key, f.type, f.defaultValue, f.importance, f.doc));

    // This definition is internal only.
    configDef.define(
        PARTITION_LIST,
        ConfigDef.Type.STRING,
        "",
        ConfigDef.Importance.HIGH,
        "The list of partitions for this task to handle");
    configDef.define(
        CONSUMER_CLIENT_ID,
        ConfigDef.Type.STRING,
        "",
        ConfigDef.Importance.HIGH,
        "Client ID used to uniquely identify the consumer in this task");
    return configDef;
  }
}
