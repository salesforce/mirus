/*
 *  Copyright (c) 2018, salesforce.com, inc.
 *  All rights reserved.
 *  SPDX-License-Identifier: BSD-3-Clause
 *  For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 *
 */

package com.salesforce.mirus.metrics;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.HashMap;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.connect.runtime.Herder;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorInfo;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorType;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class ConnectorJmxReporterTest {

  public static final String GROUP = "connector-metrics";
  @Mock private Herder herder;

  private static final String CONNECTOR_NAME = "TestConnector";
  private ConnectorJmxReporter connectorJmxReporter;
  private ConnectorInfo connectorInfo;
  private Metrics metrics;
  private HashMap<String, String> tags;

  @Before
  public void setUp() throws Exception {
    metrics = new Metrics();
    connectorInfo =
        new ConnectorInfo(CONNECTOR_NAME, new HashMap<>(), new ArrayList<>(), ConnectorType.SOURCE);
    tags = new HashMap<>();
    tags.put("connector", CONNECTOR_NAME);

    connectorJmxReporter = new ConnectorJmxReporter(metrics);
    connectorJmxReporter.handleConnector(herder, connectorInfo);
  }

  @Test
  public void testInitialValues() {
    assertEquals(
        0.0d, metrics.metric(new MetricName("failed-task-count", GROUP, "", tags)).metricValue());
  }

  @Test
  public void testIncrementTotalFailedCount() {
    assertEquals(
        0.0d,
        metrics
            .metric(new MetricName("task-failed-restart-attempts-count", GROUP, "", tags))
            .metricValue());
    connectorJmxReporter.incrementTotalFailedCount(CONNECTOR_NAME);
    assertEquals(
        1.0d,
        metrics
            .metric(new MetricName("task-failed-restart-attempts-count", GROUP, "", tags))
            .metricValue());
  }

  @Test
  public void testIncrementConnectorRestartAttempts() {
    assertEquals(
        0.0d,
        metrics
            .metric(new MetricName("connector-failed-restart-attempts-count", GROUP, "", tags))
            .metricValue());
    connectorJmxReporter.incrementConnectorRestartAttempts(CONNECTOR_NAME);
    assertEquals(
        1.0d,
        metrics
            .metric(new MetricName("connector-failed-restart-attempts-count", GROUP, "", tags))
            .metricValue());
  }
}
