/*
 *  Copyright (c) 2018, salesforce.com, inc.
 *  All rights reserved.
 *  SPDX-License-Identifier: BSD-3-Clause
 *  For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 *
 */

package com.salesforce.mirus.metrics;

import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.Value;
import org.apache.kafka.common.utils.Time;

/** JMX wrapper class to publish JMX metrics related to missing topic/partition metrics. */
public class MissingPartitionsJmxReporter extends AbstractMirusJmxReporter {

  private static final String MISSING_DEST_PARTITIONS = "missing-dest-partitions";

  private final Sensor missingPartsSensor;

  public MissingPartitionsJmxReporter() {
    this(new Metrics());
  }

  MissingPartitionsJmxReporter(Metrics metrics) {
    super(metrics);
    Sensor missingPartsSensor = metrics.sensor(MISSING_DEST_PARTITIONS);
    MetricName missingPartsName = metrics.metricName(MISSING_DEST_PARTITIONS + "-count", "mirus");
    missingPartsSensor.add(missingPartsName, new Value());
    this.missingPartsSensor = missingPartsSensor;
  }

  public void recordMetric(int missingPartitions) {
    missingPartsSensor.record(missingPartitions, Time.SYSTEM.milliseconds());
  }
}
