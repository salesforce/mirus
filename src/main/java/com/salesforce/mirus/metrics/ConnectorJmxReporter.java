/*
 *  Copyright (c) 2018, salesforce.com, inc.
 *  All rights reserved.
 *  SPDX-License-Identifier: BSD-3-Clause
 *  For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 *
 */

package com.salesforce.mirus.metrics;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.stats.Total;
import org.apache.kafka.common.metrics.stats.Value;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.connect.runtime.Herder;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorInfo;

/** JMX wrapper class to publish JMX metrics for Mirus-specific connector-related metrics. */
public class ConnectorJmxReporter extends AbstractMirusJmxReporter {

  private static final String FAILED_TASK_METRIC_NAME = "failed-task";
  private static final String PAUSED_TASK_METRIC_NAME = "paused-task";
  private static final String DESTROYED_TASK_METRIC_NAME = "destroyed-task";
  private static final String RUNNING_TASK_METRIC_NAME = "running-task";
  private static final String UNASSIGNED_TASK_METRIC_NAME = "unassigned-task";
  private static final String FAILED_TASK_ATTEMPTS_METRIC_NAME = "task-failed-restart-attempts";
  private static final String FAILED_CONNECTOR_ATTEMPTS_METRIC_NAME =
      "connector-failed-restart-attempts";
  private static final String CONNECTOR_JMX_GROUP_NAME = "connector-metrics";

  private final Set<String> connectorLevelJmxTags = new LinkedHashSet<>();
  private final Map<String, String> allStates = new HashMap<>();

  public ConnectorJmxReporter() {
    this(new Metrics());
  }

  ConnectorJmxReporter(Metrics metrics) {
    super(metrics);
    allStates.put("RUNNING", "running");
    allStates.put("FAILED", "failed");
    allStates.put("DESTROYED", "destroyed");
    allStates.put("UNASSIGNED", "unassigned");
    allStates.put("PAUSED", "paused");
    connectorLevelJmxTags.add(CONNECTOR_KEY);
  }

  public void handleConnector(Herder herder, ConnectorInfo connector) {

    ensureMetricsCreated(connector.name());
    Map<String, Long> stateCounts =
        connector
            .tasks()
            .stream()
            .map(t -> herder.taskStatus(t).state())
            .collect(Collectors.groupingBy(Function.identity(), Collectors.counting()));

    allStates.keySet().forEach(state -> stateCounts.putIfAbsent(state, 0L));

    stateCounts.forEach(
        (state, count) ->
            metrics
                .sensor(calculateSensorName(state.toLowerCase(), connector.name()))
                .record(count, Time.SYSTEM.milliseconds()));
  }

  private Map<String, String> getConnectorLevelTags(String name) {
    return Collections.singletonMap(CONNECTOR_KEY, name);
  }

  private String calculateSensorName(String state, String connectorName) {
    return state + connectorName;
  }

  private void ensureMetricsCreated(String connectorName) {

    Map<String, String> connectorTags = getConnectorLevelTags(connectorName);

    MetricName runningMetric =
        getMetric(
            RUNNING_TASK_METRIC_NAME + "-count",
            CONNECTOR_JMX_GROUP_NAME,
            "count of running tasks per connector",
            connectorLevelJmxTags,
            connectorTags);
    MetricName pausedMetric =
        getMetric(
            PAUSED_TASK_METRIC_NAME + "-count",
            CONNECTOR_JMX_GROUP_NAME,
            "count of paused tasks per connector",
            connectorLevelJmxTags,
            connectorTags);
    MetricName failedMetric =
        getMetric(
            FAILED_TASK_METRIC_NAME + "-count",
            CONNECTOR_JMX_GROUP_NAME,
            "count of failed tasks per connector",
            connectorLevelJmxTags,
            connectorTags);
    MetricName unassignedMetric =
        getMetric(
            UNASSIGNED_TASK_METRIC_NAME + "-count",
            CONNECTOR_JMX_GROUP_NAME,
            "count of unassigned tasks per connector",
            connectorLevelJmxTags,
            connectorTags);
    MetricName destroyedMetric =
        getMetric(
            DESTROYED_TASK_METRIC_NAME + "-count",
            CONNECTOR_JMX_GROUP_NAME,
            "count of destroyed tasks per connector",
            connectorLevelJmxTags,
            connectorTags);

    MetricName totalAttemptsPerConnectorMetric =
        getMetric(
            FAILED_TASK_ATTEMPTS_METRIC_NAME + "-count",
            CONNECTOR_JMX_GROUP_NAME,
            "count of failed task restart attempts per connector",
            connectorLevelJmxTags,
            connectorTags);

    MetricName restartAttemptsPerConnectorMetric =
        getMetric(
            FAILED_CONNECTOR_ATTEMPTS_METRIC_NAME + "-count",
            CONNECTOR_JMX_GROUP_NAME,
            "count of failed connector restart attempts per connector",
            connectorLevelJmxTags,
            connectorTags);

    if (!metrics.metrics().containsKey(runningMetric)) {
      metrics
          .sensor(calculateSensorName(allStates.get("RUNNING"), connectorName))
          .add(runningMetric, new Value());
    }
    if (!metrics.metrics().containsKey(pausedMetric)) {
      metrics
          .sensor(calculateSensorName(allStates.get("PAUSED"), connectorName))
          .add(pausedMetric, new Value());
    }

    if (!metrics.metrics().containsKey(failedMetric)) {
      metrics
          .sensor(calculateSensorName(allStates.get("FAILED"), connectorName))
          .add(failedMetric, new Value());
    }
    if (!metrics.metrics().containsKey(unassignedMetric)) {
      metrics
          .sensor(calculateSensorName(allStates.get("UNASSIGNED"), connectorName))
          .add(unassignedMetric, new Value());
    }
    if (!metrics.metrics().containsKey(destroyedMetric)) {
      metrics
          .sensor(calculateSensorName(allStates.get("DESTROYED"), connectorName))
          .add(destroyedMetric, new Value());
    }
    if (!metrics.metrics().containsKey(totalAttemptsPerConnectorMetric)) {
      metrics
          .sensor(FAILED_TASK_ATTEMPTS_METRIC_NAME + connectorName)
          .add(totalAttemptsPerConnectorMetric, new Total());
    }

    if (!metrics.metrics().containsKey(restartAttemptsPerConnectorMetric)) {
      metrics
          .sensor(FAILED_CONNECTOR_ATTEMPTS_METRIC_NAME + connectorName)
          .add(restartAttemptsPerConnectorMetric, new Total());
    }
  }

  public void incrementTotalFailedCount(String connector) {
    metrics
        .sensor(FAILED_TASK_ATTEMPTS_METRIC_NAME + connector)
        .record(1, Time.SYSTEM.milliseconds());
  }

  public void incrementConnectorRestartAttempts(String connector) {
    metrics
        .sensor(FAILED_CONNECTOR_ATTEMPTS_METRIC_NAME + connector)
        .record(1, Time.SYSTEM.milliseconds());
  }
}
