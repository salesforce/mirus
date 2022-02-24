package com.salesforce.mirus.metrics;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.metrics.Metrics;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class MirrorJmxReporterTest {

  private MirrorJmxReporter mirrorJmxReporter;
  private Metrics metrics;
  private final String TEST_TOPIC = "TestTopic";

  @Before
  public void setUp() throws Exception {
    mirrorJmxReporter = MirrorJmxReporter.getInstance();
    metrics = mirrorJmxReporter.metrics;
  }

  @Test
  public void updateLatencyMetrics() {
    TopicPartition topicPartition = new TopicPartition(TEST_TOPIC, 1);
    mirrorJmxReporter.addTopics(List.of(topicPartition));

    mirrorJmxReporter.recordMirrorLatency(TEST_TOPIC, 500);

    Map<String, String> tags = new LinkedHashMap<>();
    tags.put("topic", TEST_TOPIC);
    tags.put("bucket", "0m");
    Object value =
        metrics
            .metrics()
            .get(
                metrics.metricName(
                    MirrorJmxReporter.HISTOGRAM_LATENCY.name(),
                    MirrorJmxReporter.HISTOGRAM_LATENCY.group(),
                    MirrorJmxReporter.HISTOGRAM_LATENCY.description(),
                    tags))
            .metricValue();
    Assert.assertTrue((double) value > 0);

    tags.put("bucket", "12h");
    value =
        metrics
            .metrics()
            .get(
                metrics.metricName(
                    MirrorJmxReporter.HISTOGRAM_LATENCY.name(),
                    MirrorJmxReporter.HISTOGRAM_LATENCY.group(),
                    MirrorJmxReporter.HISTOGRAM_LATENCY.description(),
                    tags))
            .metricValue();
    Assert.assertTrue((double) value == 0);
  }
}
