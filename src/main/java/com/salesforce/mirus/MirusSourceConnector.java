/*
 *  Copyright (c) 2018, salesforce.com, inc.
 *  All rights reserved.
 *  SPDX-License-Identifier: BSD-3-Clause
 *  For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 *
 */

package com.salesforce.mirus;

import com.google.common.base.Charsets;
import com.google.common.io.Resources;
import com.salesforce.mirus.assignment.RoundRobinTaskAssignor;
import com.salesforce.mirus.config.SourceConfig;
import com.salesforce.mirus.config.SourceConfigDefinition;
import java.io.IOException;
import java.net.URL;
import java.util.List;
import java.util.Map;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceConnector;
import org.eclipse.jetty.io.RuntimeIOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * MirusSourceConnector is a {@link SourceConnector} implementation that uses {@link
 * MirusSourceTask} instances to mirror data between Kafka clusters.
 *
 * <p>Starts a {@link KafkaMonitor} thread and uses this to fetch current taskConfigs.
 */
public class MirusSourceConnector extends SourceConnector {

  private static final Logger logger = LoggerFactory.getLogger(MirusSourceConnector.class);
  private static final ConfigDef SOURCE_CONFIG_DEF = SourceConfigDefinition.configDef();

  private final String version;
  private Thread kafkaMonitorThread;
  private KafkaMonitor kafkaMonitor;

  @SuppressWarnings("WeakerAccess")
  public MirusSourceConnector() {
    version = readVersion();
  }

  private String readVersion() {
    try {
      URL url = Resources.getResource(MirusSourceConnector.class, "mirus-version.txt");
      return Resources.toString(url, Charsets.UTF_8);
    } catch (IOException e) {
      throw new RuntimeIOException(e);
    }
  }

  @Override
  public String version() {
    return version;
  }

  @Override
  public void start(Map<String, String> properties) {
    try {
      SourceConfig config = new SourceConfig(properties);
      TaskConfigBuilder taskConfigBuilder =
          new TaskConfigBuilder(new RoundRobinTaskAssignor(), config);
      start(new KafkaMonitor(context, config, taskConfigBuilder));
    } catch (Exception e) {
      logger.error("Unable to start KafkaMonitor", e);
      context.raiseError(e);
    }
  }

  void start(KafkaMonitor kafkaMonitor) {
    this.kafkaMonitor = kafkaMonitor;
    kafkaMonitorThread = new Thread(kafkaMonitor);
    kafkaMonitorThread.setName("kafka-monitor");
    kafkaMonitorThread.start();
  }

  @Override
  public Class<? extends Task> taskClass() {
    return MirusSourceTask.class;
  }

  @Override
  public List<Map<String, String>> taskConfigs(int maxTasks) {
    return kafkaMonitor.taskConfigs(maxTasks);
  }

  @Override
  public void stop() {
    if (kafkaMonitorThread != null) {
      kafkaMonitor.stop();
      try {
        kafkaMonitorThread.join(10000);
        if (kafkaMonitorThread.isAlive()) {
          logger.error("Unable to shut down KafkaMonitor");
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
  }

  @Override
  public ConfigDef config() {
    return SOURCE_CONFIG_DEF;
  }
}
