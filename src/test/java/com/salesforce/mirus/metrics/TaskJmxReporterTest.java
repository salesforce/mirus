/*
 *  Copyright (c) 2018, salesforce.com, inc.
 *  All rights reserved.
 *  SPDX-License-Identifier: BSD-3-Clause
 *  For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 *
 */

package com.salesforce.mirus.metrics;

import static org.junit.Assert.assertEquals;

import java.util.HashMap;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorStateInfo.TaskState;
import org.apache.kafka.connect.util.ConnectorTaskId;
import org.junit.Before;
import org.junit.Test;

public class TaskJmxReporterTest {

  public static final String GROUP = "connector-task-metrics";

  private static final String CONNECTOR_NAME = "TestConnector";
  private TaskJmxReporter taskJmxReporter;
  private Metrics metrics;

  @Before
  public void setUp() throws Exception {
    metrics = new Metrics();

    taskJmxReporter = new TaskJmxReporter(metrics);
  }

  private void assertFailedMetricCount(String state, int task, Double expected) {
    ConnectorTaskId taskId = new ConnectorTaskId(CONNECTOR_NAME, task);
    TaskState taskState = new TaskState(task, state, "worker1", "Test");
    taskJmxReporter.updateMetrics(taskId, taskState);

    HashMap<String, String> tags = new HashMap<>();
    tags.put("connector", CONNECTOR_NAME);
    tags.put("task", Integer.toString(task));

    assertEquals(
        expected,
        metrics
            .metric(new MetricName("task-failed-restart-attempts-count", GROUP, "", tags))
            .metricValue());
  }

  @Test
  public void testUpdateMetrics() {
    assertFailedMetricCount("RUNNING", 1, 0.0);
    assertFailedMetricCount("FAILED", 1, 1.0);
    assertFailedMetricCount("FAILED", 1, 2.0);
    assertFailedMetricCount("FAILED", 2, 1.0);
  }
}
