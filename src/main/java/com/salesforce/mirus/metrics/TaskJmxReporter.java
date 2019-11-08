/*
 *  Copyright (c) 2018, salesforce.com, inc.
 *  All rights reserved.
 *  SPDX-License-Identifier: BSD-3-Clause
 *  For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 *
 */

package com.salesforce.mirus.metrics;

import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.Total;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.connect.runtime.TaskStatus;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorStateInfo.TaskState;
import org.apache.kafka.connect.util.ConnectorTaskId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * JMX wrapper class to publish Mirus-specific JMX metrics for task-related metrics. Right now, it
 * publishes metrics related to Failed Restart Attempts.
 */
public class TaskJmxReporter extends AbstractMirusJmxReporter {

  private static final Logger logger = LoggerFactory.getLogger(TaskJmxReporter.class);

  private static final String FAILED_TASK_ATTEMPTS_METRIC_NAME = "task-failed-restart-attempts";
  private static final String TASK_CONNECTOR_JMX_GROUP_NAME = "connector-task-metrics";

  private final Set<String> taskLevelJmxTags = new LinkedHashSet<>();

  public TaskJmxReporter() {
    this(new Metrics());
  }

  public TaskJmxReporter(Metrics metrics) {
    super(metrics);
    // Order of additions is important since it defines the JMX metric hierarchy.
    taskLevelJmxTags.add(CONNECTOR_KEY);
    taskLevelJmxTags.add(TASK_KEY);
  }

  public void updateMetrics(ConnectorTaskId taskId, TaskState taskStatus) {
    ensureMetricsCreated(taskId);
    updateTaskMetrics(taskId, taskStatus);
  }

  private void ensureMetricsCreated(ConnectorTaskId taskId) {
    Map<String, String> tags = getTaskLevelTags(taskId);
    MetricName taskMetric =
        getMetric(
            FAILED_TASK_ATTEMPTS_METRIC_NAME + "-count",
            TASK_CONNECTOR_JMX_GROUP_NAME,
            "count of restart attempts to a failed task",
            taskLevelJmxTags,
            tags);

    if (!metrics.metrics().containsKey(taskMetric)) {
      Sensor sensor = getSensor(taskId.toString());
      sensor.add(taskMetric, new Total());
      logger.info("Added the task {} to the list of JMX metrics", taskId);
      logger.debug("Updated set of JMX metrics is {}", metrics.metrics());
    }
  }

  private Map<String, String> getTaskLevelTags(ConnectorTaskId taskId) {
    Map<String, String> tags = new LinkedHashMap<>();
    tags.put(CONNECTOR_KEY, taskId.connector());
    tags.put(TASK_KEY, Integer.toString(taskId.task()));
    return tags;
  }

  private Sensor getSensor(String taskId) {
    return metrics.sensor(taskId);
  }

  private void updateTaskMetrics(ConnectorTaskId taskId, TaskState taskStatus) {

    if (taskStatus.state().equalsIgnoreCase(TaskStatus.State.FAILED.toString())) {
      Sensor sensor = getSensor(taskId.toString());
      sensor.record(1, Time.SYSTEM.milliseconds());
    }
  }
}
