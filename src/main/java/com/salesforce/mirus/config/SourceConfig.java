/*
 *  Copyright (c) 2018, salesforce.com, inc.
 *  All rights reserved.
 *  SPDX-License-Identifier: BSD-3-Clause
 *  For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 *
 */

package com.salesforce.mirus.config;

import java.util.List;
import java.util.Map;
import org.apache.kafka.connect.transforms.util.SimpleConfig;

public class SourceConfig {

  private final SimpleConfig simpleConfig;

  public SourceConfig(Map<String, String> properties) {
    this.simpleConfig = new SimpleConfig(SourceConfigDefinition.configDef(), properties);
  }

  public List<String> getTopicsWhitelist() {
    return simpleConfig.getList(SourceConfigDefinition.TOPICS_WHITELIST.key);
  }

  public Long getMonitorPollWaitMs() {
    return simpleConfig.getLong(SourceConfigDefinition.MONITOR_POLL_WAIT_MS.key);
  }

  public String getTopicsRegex() {
    return simpleConfig.getString(SourceConfigDefinition.TOPICS_REGEX.key);
  }

  public Map<String, Object> getConsumerProperties() {
    return simpleConfig.originalsWithPrefix("consumer.");
  }

  public String getDestinationBootstrapServers() {
    return simpleConfig.getString(SourceConfigDefinition.DESTINATION_BOOTSTRAP_SERVERS.key);
  }

  public boolean getEnablePartitionMatching() {
    return simpleConfig.getBoolean(SourceConfigDefinition.ENABLE_PARTITION_MATCHING.key);
  }

  public String getName() {
    return simpleConfig.getString(SourceConfigDefinition.NAME.key);
  }

  public Map<String, String> originalsStrings() {
    return simpleConfig.originalsStrings();
  }

  public boolean getEnableBufferFlushing() {
    return simpleConfig.getBoolean(SourceConfigDefinition.ENABLE_BUFFER_FLUSHING.key);
  }
}
