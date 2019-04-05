/*
 *  Copyright (c) 2018, salesforce.com, inc.
 *  All rights reserved.
 *  SPDX-License-Identifier: BSD-3-Clause
 *  For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 *
 */

package com.salesforce.mirus.config;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.runtime.ConnectorConfig;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SimpleConfig;

public class SourceConfig {

  private final SimpleConfig simpleConfig;
  private List<Transformation<SourceRecord>> transformations;

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

  public boolean getTopicCheckingEnabled() {
    return simpleConfig.getBoolean(SourceConfigDefinition.ENABLE_DESTINATION_TOPIC_CHECKING.key);
  }

  public String getName() {
    return simpleConfig.getString(ConnectorConfig.NAME_CONFIG);
  }

  public Map<String, String> originalsStrings() {
    return simpleConfig.originalsStrings();
  }

  public List<Transformation<SourceRecord>> transformations() {
    if (this.transformations == null) {
      this.transformations = buildTransformations();
    }
    return this.transformations;
  }

  private List<Transformation<SourceRecord>> buildTransformations() {
    List<Transformation<SourceRecord>> transformations = new ArrayList<>();
    List<String> transformNames = simpleConfig.getList(ConnectorConfig.TRANSFORMS_CONFIG);
    for (String name : transformNames) {
      String configPrefix = ConnectorConfig.TRANSFORMS_CONFIG + "." + name + ".";

      // We don't have access to Plugins to properly add loaded classes' configs to the definition,
      // so retrieve it based on the transform prefix.
      Map<String, Object> transformConfig = simpleConfig.originalsWithPrefix(configPrefix);
      String transformClassName = (String) transformConfig.get("type");

      Transformation<SourceRecord> transform;
      try {
        Class<?> transformClass =
            (Class<?>)
                ConfigDef.parseType(
                    configPrefix + "type", transformClassName, ConfigDef.Type.CLASS);
        transform = transformClass.asSubclass(Transformation.class).newInstance();
        transform.configure(transformConfig);
      } catch (RuntimeException | InstantiationException | IllegalAccessException e) {
        // If we couldn't build and configure the Transformation properly we can't verify
        // that we'll be looking for the right target topics, so throw an error.
        throw new ConnectException(
            String.format("Error building transformation %s from config", name), e);
      }
      transformations.add(transform);
    }
    return transformations;
  }
}
