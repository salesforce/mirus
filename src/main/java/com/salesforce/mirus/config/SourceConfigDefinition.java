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

/**
 * Properties listed here can be applied to the MirusSourceConnector configuration object, which is
 * submitted as a JSON object to the REST Config API. These include details of the cluster the
 * {@link com.salesforce.mirus.MirusSourceConnector} will mirror data from, along with some
 * information on the destination cluster for topic metadata validation.
 */
public enum SourceConfigDefinition {
  NAME("name", ConfigDef.Type.STRING, "", ConfigDef.Importance.HIGH, "Unique name for this source"),
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
  DESTINATION_BOOTSTRAP_SERVERS(
      "destination.bootstrap.servers",
      ConfigDef.Type.STRING,
      "",
      ConfigDef.Importance.HIGH,
      "Comma-separated list of destination bootstrap server endpoints in standard format. This is used by the Kafka Monitor for run-time topic validation"),
  ENABLE_BUFFER_FLUSHING(
      "enable.buffer.flushing",
      ConfigDef.Type.BOOLEAN,
      false,
      ConfigDef.Importance.MEDIUM,
      "Boolean to decide whether Producer buffer should flush when it gets too full or not");

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
    return configDef;
  }

  public String getKey() {
    return key;
  }
}
