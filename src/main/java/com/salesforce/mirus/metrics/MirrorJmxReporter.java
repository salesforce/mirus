package com.salesforce.mirus.metrics;

import com.google.common.collect.Sets;
import com.salesforce.mirus.MirusSourceConnector;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.kafka.common.MetricNameTemplate;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MirrorJmxReporter extends AbstractMirusJmxReporter {

  private static final Logger logger = LoggerFactory.getLogger(MirrorJmxReporter.class);

  public static Map<Integer, String> LATENCY_BUCKETS =
      Map.of(
          0,
          "0m",
          5 * 60 * 1000,
          "5m",
          10 * 60 * 1000,
          "10m",
          30 * 60 * 1000,
          "30m",
          60 * 60 * 1000,
          "60m");

  private static MirrorJmxReporter instance = null;

  private static final String SOURCE_CONNECTOR_GROUP = MirusSourceConnector.class.getSimpleName();

  private static final Set<String> TOPIC_TAGS = new HashSet<>(Collections.singletonList("topic"));
  private static final Set<String> TOPIC_BUCKET_TAGS = Sets.newHashSet("topic", "bucket");

  private static final MetricNameTemplate REPLICATION_LATENCY =
      new MetricNameTemplate(
          "replication-latency-ms", SOURCE_CONNECTOR_GROUP,
          "Time it takes records to replicate from source to target cluster.", TOPIC_TAGS);
  private static final MetricNameTemplate REPLICATION_LATENCY_MAX =
      new MetricNameTemplate(
          "replication-latency-ms-max", SOURCE_CONNECTOR_GROUP,
          "Max time it takes records to replicate from source to target cluster.", TOPIC_TAGS);
  private static final MetricNameTemplate REPLICATION_LATENCY_MIN =
      new MetricNameTemplate(
          "replication-latency-ms-min", SOURCE_CONNECTOR_GROUP,
          "Min time it takes records to replicate from source to target cluster.", TOPIC_TAGS);
  private static final MetricNameTemplate REPLICATION_LATENCY_AVG =
      new MetricNameTemplate(
          "replication-latency-ms-avg", SOURCE_CONNECTOR_GROUP,
          "Average time it takes records to replicate from source to target cluster.", TOPIC_TAGS);

  private static final MetricNameTemplate HISTOGRAM_LATENCY =
      new MetricNameTemplate(
          "histogram-bucket-latency",
          SOURCE_CONNECTOR_GROUP,
          "Metrics counting the number of records produced in each of a small set of latency buckets.",
          TOPIC_BUCKET_TAGS);

  // Map of topics to their metric objects
  private final Map<String, Sensor> topicSensors;
  private final Set<TopicPartition> topicPartitionSet;
  private final Map<String, Map<Integer, Sensor>> histogramLatencySensors;

  private MirrorJmxReporter() {
    super(new Metrics());
    metrics.sensor("replication-latency");

    topicSensors = new HashMap<>();
    topicPartitionSet = new HashSet<>();
    histogramLatencySensors = new HashMap<>();

    logger.info("Initialized MirrorJMXReporter");
  }

  public static synchronized MirrorJmxReporter getInstance() {
    if (null == instance) {
      instance = new MirrorJmxReporter();
    }
    return instance;
  }

  /**
   * Add a list of partitions to record the latency of.
   *
   * @param topicPartitions
   */
  public synchronized void addTopics(List<TopicPartition> topicPartitions) {
    topicSensors.putAll(
        topicPartitions
            .stream()
            .map(TopicPartition::topic)
            .distinct()
            .filter(topic -> !topicSensors.containsKey(topic))
            .collect(Collectors.toMap(topic -> topic, this::createTopicSensor)));
    topicPartitionSet.addAll(topicPartitions);

    for (TopicPartition topicPartition : topicPartitions) {
      Map<Integer, Sensor> bucketSensors = new HashMap<>();
      String topic = topicPartition.topic();
      for (Map.Entry<Integer, String> entry : LATENCY_BUCKETS.entrySet()) {
        bucketSensors.put(entry.getKey(), createHistogramSensor(topic, entry.getValue()));
      }
      histogramLatencySensors.put(topic, bucketSensors);
    }
  }

  /**
   * Remove a list of partitions from the metrics, this is involved since we want to record if a
   * single partition of a topic is held by this worker.
   *
   * @param topicPartitions
   */
  public synchronized void removeTopics(List<TopicPartition> topicPartitions) {
    // We want to remove all topics that are passed in topicPartitions
    // However, we need to be smart since the topic may still be "owned" by a different partition.

    topicPartitionSet.removeAll(topicPartitions);

    // Get all the distinct topic names that should still exist in the sensors.
    // Then subtract that set from topics to remove.
    // We don't need calls to "distinct" since we are operating with sets of strings.
    Set<String> currentTopics =
        topicPartitionSet.stream().map(TopicPartition::topic).collect(Collectors.toSet());

    Set<String> topicsToRemove =
        topicPartitions
            .stream()
            .map(TopicPartition::topic)
            .filter(topic -> !currentTopics.contains(topic))
            .collect(Collectors.toSet());

    topicsToRemove.forEach(
        topic -> {
          metrics.removeSensor(replicationLatencySensorName(topic));
          topicSensors.remove(topic);
          histogramLatencySensors.remove(topic);
        });
  }

  public synchronized void recordMirrorLatency(String topic, long millis) {
    Sensor sensor = topicSensors.get(topic);
    if (sensor != null) {
      sensor.record((double) millis);
    }

    Map<Integer, Sensor> bucketSensors = histogramLatencySensors.get(topic);
    bucketSensors
        .entrySet()
        .stream()
        .forEach(
            sensorEntry -> {
              if (millis > sensorEntry.getKey()) {
                sensorEntry.getValue().record(1);
              }
            });
  }

  private Sensor createTopicSensor(String topic) {

    Map<String, String> tags = new LinkedHashMap<>();
    tags.put("topic", topic);

    Sensor sensor = metrics.sensor(replicationLatencySensorName(topic));
    sensor.add(metrics.metricInstance(REPLICATION_LATENCY, tags), new Value());
    sensor.add(metrics.metricInstance(REPLICATION_LATENCY_MAX, tags), new Max());
    sensor.add(metrics.metricInstance(REPLICATION_LATENCY_MIN, tags), new Min());
    sensor.add(metrics.metricInstance(REPLICATION_LATENCY_AVG, tags), new Avg());
    return sensor;
  }

  private Sensor createHistogramSensor(String topic, String bucket) {
    Map<String, String> tags = new LinkedHashMap<>();
    tags.put("topic", topic);
    tags.put("bucket", bucket);

    Sensor sensor = metrics.sensor(histogramLatencySensorName(topic, bucket));
    sensor.add(
        metrics.metricInstance(HISTOGRAM_LATENCY, tags),
        new Rate(TimeUnit.SECONDS, new WindowedSum()));

    return sensor;
  }

  private String replicationLatencySensorName(String topic) {
    return topic + "-" + "replication-latency";
  }

  private String histogramLatencySensorName(String topic, String bucket) {
    return topic + "-" + bucket + "-" + "histogram-latency";
  }
}
