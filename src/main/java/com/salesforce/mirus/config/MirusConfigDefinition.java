/*
 * Copyright (c) 2018, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause For full license text, see the LICENSE
 * file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.mirus.config;

import org.apache.kafka.common.config.ConfigDef;

/**
 * Configuration properties listed here are extensions to the standard Kafka Connect worker
 * properties {@link org.apache.kafka.connect.runtime.WorkerConfig}. They can appear in the the
 * worker properties file provided to the framework at startup.
 */
public enum MirusConfigDefinition {

  /**
   * Task Monitor configuration. The task monitor reports task status count metrics to JMX, and can
   * automatically restart failed tasks.
   */
  FAILED_TASK_POLLING_CYCLE(
      "mirus.task.monitor.polling.interval.ms",
      ConfigDef.Type.LONG,
      120000,
      ConfigDef.Importance.MEDIUM,
      "Task monitor polling interval"),
  AUTO_RESTART_TASK_ENABLED(
      "mirus.task.auto.restart.enabled",
      ConfigDef.Type.BOOLEAN,
      true,
      ConfigDef.Importance.MEDIUM,
      "When enabled tasks in FAILED status will be restarted automatically"),
  AUTO_RESTART_CONNECTOR_ENABLED(
      "mirus.connector.auto.restart.enabled",
      ConfigDef.Type.BOOLEAN,
      true,
      ConfigDef.Importance.MEDIUM,
      "Boolean to decide whether failed connectors should be restarted automatically or not");

  String key;
  ConfigDef.Type type;
  Object defaultValue;
  ConfigDef.Importance importance;
  String doc;

  MirusConfigDefinition(
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
    for (MirusConfigDefinition f : MirusConfigDefinition.values()) {
      configDef = configDef.define(f.key, f.type, f.defaultValue, f.importance, f.doc);
    }
    return configDef;
  }

  public String getKey() {
    return key;
  }
}
