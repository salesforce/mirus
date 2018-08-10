/*
 *  Copyright (c) 2018, salesforce.com, inc.
 *  All rights reserved.
 *  SPDX-License-Identifier: BSD-3-Clause
 *  For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 *
 */

package com.salesforce.mirus;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.salesforce.mirus.config.MirusConfig;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.connect.runtime.Connect;
import org.apache.kafka.connect.runtime.Worker;
import org.apache.kafka.connect.runtime.WorkerConfig;
import org.apache.kafka.connect.runtime.distributed.DistributedConfig;
import org.apache.kafka.connect.runtime.distributed.DistributedHerder;
import org.apache.kafka.connect.runtime.isolation.Plugins;
import org.apache.kafka.connect.runtime.rest.RestServer;
import org.apache.kafka.connect.storage.ConfigBackingStore;
import org.apache.kafka.connect.storage.KafkaConfigBackingStore;
import org.apache.kafka.connect.storage.KafkaOffsetBackingStore;
import org.apache.kafka.connect.storage.KafkaStatusBackingStore;
import org.apache.kafka.connect.storage.StatusBackingStore;
import org.apache.kafka.connect.util.ConnectUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Mirus provides a custom Kafka Connect entry point. While MirusSourceConnector and MirusSourceTask
 * are compatible with the standard Kafka Connect entry point, this class offers some extensions to
 * improve operational deployment:
 *
 * <ul>
 *   <li>Supports worker property value overrides, in the same format as the Kafka Server, to
 *       simplify configuration
 *   <li>Ensure client.id for internal Kafka clients use unique names by adding suffixes
 * </ul>
 */
public class Mirus {

  private static final Logger logger = LoggerFactory.getLogger(Mirus.class);

  public static void main(String[] argv) throws Exception {
    Mirus.Args args = new Mirus.Args();
    JCommander jCommander =
        JCommander.newBuilder()
            .programName(OffsetStatus.class.getSimpleName())
            .addObject(args)
            .build();
    try {
      jCommander.parse(argv);
    } catch (Exception e) {
      jCommander.usage();
      throw e;
    }
    if (args.help) {
      jCommander.usage();
      System.exit(1);
    }

    Map<String, String> workerProps =
        !args.workerPropertiesFile.isEmpty()
            ? Utils.propsToStringMap(Utils.loadProps(args.workerPropertiesFile))
            : Collections.emptyMap();

    applyOverrides(args.overrides, workerProps);

    startConnect(workerProps);
  }

  static void applyOverrides(List<String> overrides, Map<String, String> properties)
      throws IOException {
    // Read the override strings using the standard properties class.
    Properties newProperties = new Properties();
    for (String f : overrides) {
      newProperties.load(new ByteArrayInputStream(f.getBytes(StandardCharsets.UTF_8)));
    }

    Enumeration<?> propertyNames = newProperties.propertyNames();
    while (propertyNames.hasMoreElements()) {
      String propertyName = (String) propertyNames.nextElement();
      properties.put(propertyName, (String) newProperties.get(propertyName));
    }
  }

  /**
   * Create a new DistributedConfig object with a suffix applied to the client id. This allows us to
   * make the client id unique so JMX metrics work properly.
   */
  private static DistributedConfig configWithClientIdSuffix(
      Map<String, String> workerProps, String suffix) {
    Map<String, String> localProps = new HashMap<>(workerProps);
    localProps.computeIfPresent(CommonClientConfigs.CLIENT_ID_CONFIG, (k, v) -> v + suffix);
    return new DistributedConfig(localProps);
  }

  /**
   * This method is based on the the standard Kafka Connect start logic as in ConnectDistributed,
   * but with `clientid` prefix support, to prevent JMX metric names from clashing. Also supports
   * command-line property overrides (useful for run-time port configuration), and starts the Mirus
   * `TaskMonitor`.
   */
  private static void startConnect(Map<String, String> workerProps) {
    Time time = Time.SYSTEM;
    MirusConfig mirusConfig = new MirusConfig(workerProps);
    Plugins plugins = new Plugins(workerProps);
    plugins.compareAndSwapWithDelegatingLoader();

    RestServer rest = new RestServer(configWithClientIdSuffix(workerProps, "rest"));
    URI advertisedUrl = rest.advertisedUrl();
    String workerId = advertisedUrl.getHost() + ":" + advertisedUrl.getPort();

    KafkaOffsetBackingStore offsetBackingStore = new KafkaOffsetBackingStore();
    offsetBackingStore.configure(configWithClientIdSuffix(workerProps, "offset"));

    WorkerConfig workerConfigs = configWithClientIdSuffix(workerProps, "worker");
    Worker worker = new Worker(workerId, time, plugins, workerConfigs, offsetBackingStore);

    StatusBackingStore statusBackingStore =
        new KafkaStatusBackingStore(time, worker.getInternalValueConverter());
    statusBackingStore.configure(configWithClientIdSuffix(workerProps, "status"));

    ConfigBackingStore configBackingStore =
        new KafkaConfigBackingStore(
            worker.getInternalValueConverter(), configWithClientIdSuffix(workerProps, "config"));

    DistributedConfig distributedConfig = configWithClientIdSuffix(workerProps, "herder");
    String kafkaClusterId = ConnectUtils.lookupKafkaClusterId(distributedConfig);
    logger.debug("Kafka cluster ID: {}", kafkaClusterId);

    DistributedHerder herder =
        new DistributedHerder(
            distributedConfig,
            time,
            worker,
            kafkaClusterId,
            statusBackingStore,
            configBackingStore,
            advertisedUrl.toString());
    final Connect connect = new Connect(herder, rest);

    boolean autoStartTasks = mirusConfig.getTaskAutoRestart();
    boolean autoStartConnectors = mirusConfig.getConnectorAutoRestart();
    long pollingCycle = mirusConfig.getTaskStatePollingInterval();
    HerderStatusMonitor herderStatusMonitor =
        new HerderStatusMonitor(
            herder, workerId, pollingCycle, autoStartTasks, autoStartConnectors);
    Thread herderStatusMonitorThread = new Thread(herderStatusMonitor);
    herderStatusMonitorThread.setName("herder-status-monitor");
    try {
      connect.start();
    } catch (Exception e) {
      logger.error("Failed to start Connect", e);
      connect.stop();
    }
    herderStatusMonitorThread.start();
    // Shutdown will be triggered by Ctrl-C or via HTTP shutdown request
    connect.awaitStop();
  }

  static class Args {

    @Parameter(description = "Worker properties file name")
    String workerPropertiesFile = "";

    @Parameter(
        names = {"--override"},
        description = "Override a property [--override property=value]*")
    List<String> overrides = new ArrayList<>();

    @Parameter(names = "--help", help = true)
    boolean help = false;
  }
}
