/*
 *  Copyright (c) 2018, salesforce.com, inc.
 *  All rights reserved.
 *  SPDX-License-Identifier: BSD-3-Clause
 *  For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 *
 */

package com.salesforce.mirus.config;

import java.util.Collections;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.ConfigKey;
import org.apache.kafka.connect.runtime.ConnectorConfig;

/**
 * Properties listed here can be applied to the MirusSourceConnector configuration object, which is
 * submitted as a JSON object to the REST Config API. These include details of the cluster the
 * {@link com.salesforce.mirus.MirusSourceConnector} will mirror data from, along with some
 * information on the destination cluster for topic metadata validation.
 */
public enum SourceConfigDefinition {
  TOPICS_WHITELIST(
      "topics.whitelist",
      ConfigDef.Type.LIST,
      Collections.EMPTY_LIST,
      ConfigDef.Importance.HIGH,
      "Whitelisted topic names will be mirrored. Comma-separated list."),
  TOPICS_REGEX(
      "topics.regex",
      ConfigDef.Type.STRING,
      "",
      ConfigDef.Importance.HIGH,
      "Java regex whitelist.  Matching topics names will be mirrored"),
  MONITOR_POLL_WAIT_MS(
      "monitor.poll.wait.ms",
      ConfigDef.Type.LONG,
      TimeUnit.MILLISECONDS.convert(60, TimeUnit.SECONDS),
      ConfigDef.Importance.HIGH,
      "Wait time between attempts to poll for source configuration changes (milliseconds)"),
  POLL_TIMEOUT_MS(
      "poll.timeout.ms",
      ConfigDef.Type.LONG,
      TimeUnit.MILLISECONDS.convert(1, TimeUnit.SECONDS),
      ConfigDef.Importance.HIGH,
      "Timeout on Kafka consumer poll call (milliseconds)"),
  DESTINATION_TOPIC_NAME_PREFIX(
      "destination.topic.name.prefix",
      ConfigDef.Type.STRING,
      "",
      ConfigDef.Importance.MEDIUM,
      "Prefix all destination topics names with this string"),
  DESTINATION_TOPIC_NAME_SUFFIX(
      "destination.topic.name.suffix",
      ConfigDef.Type.STRING,
      "",
      ConfigDef.Importance.MEDIUM,
      "Suffix all destination topics names with this string"),
  ENABLE_PARTITION_MATCHING(
      "enable.partition.matching",
      ConfigDef.Type.BOOLEAN,
      false,
      ConfigDef.Importance.MEDIUM,
      "Ensures records are written to the destination partition with the same identifier as the source partition"),
  ENABLE_DESTINATION_TOPIC_CHECKING(
      "enable.destination.topic.checking",
      ConfigDef.Type.BOOLEAN,
      true,
      ConfigDef.Importance.LOW,
      "Enables destination topic checking to ensure the topic exists in the destination cluster."
          + " Supports the RegexRouter SMT but not other Router transformations or other topic-rerouting"
          + " transformations. Disable to use other Kafka Connect Transformations to reroute messages"
          + "to different topics."),
  SOURCE_KEY_CONVERTER(
      "source.key.converter",
      ConfigDef.Type.CLASS,
      "org.apache.kafka.connect.converters.ByteArrayConverter",
      ConfigDef.Importance.MEDIUM,
      "Converter class to apply to source record keys"),
  SOURCE_VALUE_CONVERTER(
      "source.value.converter",
      ConfigDef.Type.CLASS,
      "org.apache.kafka.connect.converters.ByteArrayConverter",
      ConfigDef.Importance.MEDIUM,
      "Converter class to apply to source record values"),
  SOURCE_HEADER_CONVERTER(
      "source.header.converter",
      ConfigDef.Type.CLASS,
      "org.apache.kafka.connect.converters.ByteArrayConverter",
      ConfigDef.Importance.MEDIUM,
      "Converter class to apply to source record headers"),
  DESTINATION_BOOTSTRAP_SERVERS(
          "destination.bootstrap.servers",
          ConfigDef.Type.STRING,
          "",
          ConfigDef.Importance.HIGH,
          "Comma-separated list of destination bootstrap server endpoints in standard format. This is used by the Kafka Monitor for run-time topic validation");

  String key;
  ConfigDef.Type type;
  Object defaultValue;
  ConfigDef.Importance importance;
  String doc;

  SourceConfigDefinition(
      String key,
      ConfigDef.Type type,
      Object defaultValue,
      ConfigDef.Importance importance,
      String doc) {
    this.key = key;
    this.type = type;
    this.defaultValue = defaultValue;
    this.importance = importance;
    this.doc = doc;
  }

  public static ConfigDef configDef() {
    ConfigDef configDef = new ConfigDef();
    for (SourceConfigDefinition f : SourceConfigDefinition.values()) {
      configDef = configDef.define(f.key, f.type, f.defaultValue, f.importance, f.doc);
    }

    // Share name and transforms config definitions from ConnectorConfig
    for (ConfigKey key : ConnectorConfig.configDef().configKeys().values()) {
      if ("Transforms".equals(key.group) || ConnectorConfig.NAME_CONFIG.equals(key.name)) {
        configDef.define(key);
      }
    }
    return configDef;
  }

  public String getKey() {
    return key;
  }
}
