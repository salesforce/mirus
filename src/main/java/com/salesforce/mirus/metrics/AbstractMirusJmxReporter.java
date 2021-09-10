/*
 *  Copyright (c) 2018, salesforce.com, inc.
 *  All rights reserved.
 *  SPDX-License-Identifier: BSD-3-Clause
 *  For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 *
 */

package com.salesforce.mirus.metrics;

import java.util.Map;
import java.util.Set;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.MetricNameTemplate;
import org.apache.kafka.common.metrics.JmxReporter;
import org.apache.kafka.common.metrics.Metrics;

abstract class AbstractMirusJmxReporter implements AutoCloseable {

  static final String CONNECTOR_KEY = "connector";
  static final String TASK_KEY = "task";

  final Metrics metrics;

  AbstractMirusJmxReporter(Metrics metrics) {
    this.metrics = metrics;
    this.metrics.addReporter(new JmxReporter("mirus"));
  }

  protected MetricName getMetric(
      String name, String group, String desc, Set<String> tags, Map<String, String> runtimeTags) {
    MetricNameTemplate failedTaskCountTemplate = new MetricNameTemplate(name, group, desc, tags);

    return metrics.metricInstance(failedTaskCountTemplate, runtimeTags);
  }

  @Override
  public void close() {
    metrics.close();
  }
}
