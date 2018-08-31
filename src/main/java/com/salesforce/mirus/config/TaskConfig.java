/*
 *  Copyright (c) 2018, salesforce.com, inc.
 *  All rights reserved.
 *  SPDX-License-Identifier: BSD-3-Clause
 *  For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 *
 */

package com.salesforce.mirus.config;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.connect.transforms.util.SimpleConfig;

/**
 * Internal task-level configuration sent by {@link com.salesforce.mirus.MirusSourceConnector} to
 * {@link com.salesforce.mirus.MirusSourceTask} instances.
 */
public class TaskConfig {

  private final SimpleConfig simpleConfig;

  public TaskConfig(Map<String, String> properties) {
    this.simpleConfig = new SimpleConfig(TaskConfigDefinition.configDef(), properties);
  }

  private static Set<String> keys() {
    return TaskConfigDefinition.configDef().names();
  }

  /**
   * Filter to remove any properties not used by TaskConfig
   *
   * @param properties the incoming Source configuration properties
   * @return the properties accepted by TaskConfig
   */
  public static Map<String, String> filterProperties(Map<String, String> properties) {
    Map<String, String> result = new HashMap<>();
    Set<String> taskKeys = keys();
    properties
        .entrySet()
        .stream()
        .filter(e -> taskKeys.contains(e.getKey()) || e.getKey().startsWith("consumer."))
        .forEach(e -> result.put(e.getKey(), e.getValue()));
    return result;
  }

  public Map<String, Object> getConsumerProperties() {
    return simpleConfig.originalsWithPrefix("consumer.");
  }

  public long getConsumerPollTimeout() {
    return simpleConfig.getLong(SourceConfigDefinition.POLL_TIMEOUT_MS.key);
  }

  public String getDestinationTopicNamePrefix() {
    return simpleConfig.getString(SourceConfigDefinition.DESTINATION_TOPIC_NAME_PREFIX.key);
  }

  public String getDestinationTopicNameSuffix() {
    return simpleConfig.getString(SourceConfigDefinition.DESTINATION_TOPIC_NAME_SUFFIX.key);
  }

  public String getInternalTaskPartitions() {
    return simpleConfig.getString(TaskConfigDefinition.PARTITION_LIST);
  }

  public boolean getEnablePartitionMatching() {
    return simpleConfig.getBoolean(SourceConfigDefinition.ENABLE_PARTITION_MATCHING.key);
  }
}
