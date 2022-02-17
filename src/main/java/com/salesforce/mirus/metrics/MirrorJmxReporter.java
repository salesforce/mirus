package com.salesforce.mirus.metrics;

import com.google.common.collect.Sets;
import com.salesforce.mirus.MirusSourceConnector;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.kafka.common.MetricNameTemplate;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.*;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MirrorJmxReporter extends AbstractMirusJmxReporter {

  private static final Logger logger = LoggerFactory.getLogger(MirrorJmxReporter.class);

  public static final Map<Long, String> LATENCY_BUCKETS =
      Map.of(
          TimeUnit.MINUTES.toMillis(0),
          "0m",
          TimeUnit.MINUTES.toMillis(5),
          "5m",
          TimeUnit.MINUTES.toMillis(10),
          "10m",
          TimeUnit.MINUTES.toMillis(30),
          "30m",
          TimeUnit.MINUTES.toMillis(60),
          "60m",
          TimeUnit.HOURS.toMillis(12),
          "12h");

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

  protected static final MetricNameTemplate HISTOGRAM_LATENCY =
      new MetricNameTemplate(
          "replication-latency-histogram",
          SOURCE_CONNECTOR_GROUP,
          "Cumulative histogram counting records delivered per second with latency exceeding a set of fixed bucket thresholds.",
          TOPIC_BUCKET_TAGS);

  // Map of topics to their metric objects
  private final Map<String, Sensor> topicSensors;
  private final Set<TopicPartition> topicPartitionSet;
  private final Map<String, Map<Long, Sensor>> histogramLatencySensors;

  private MirrorJmxReporter() {
    super(new Metrics(new MetricConfig(), new ArrayList<>(0), Time.SYSTEM, true));
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
      Map<Long, Sensor> bucketSensors = new HashMap<>();
      String topic = topicPartition.topic();
      LATENCY_BUCKETS.forEach(
          (edgeMillis, bucketName) ->
              bucketSensors.put(edgeMillis, createHistogramSensor(topic, bucketName)));
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

    Map<Long, Sensor> bucketSensors = histogramLatencySensors.get(topic);
    bucketSensors.forEach(
        (edgeMillis, bucketSensor) -> {
          if (millis >= edgeMillis) {
            if (bucketSensor.hasExpired()) {
              String bucket = LATENCY_BUCKETS.get(edgeMillis);
              // explicitly replace the expired sensor with a new one
              metrics.removeSensor(histogramLatencySensorName(topic, bucket));
              bucketSensor = createHistogramSensor(topic, bucket);
            }
            bucketSensor.record(1);
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

    // bucket sensor will be expired after 5 mins if inactive
    // this is to prevent inactive bucket sensors from reporting too many zero value metrics
    Sensor sensor =
        metrics.sensor(
            histogramLatencySensorName(topic, bucket),
            null,
            TimeUnit.MINUTES.toSeconds(5),
            Sensor.RecordingLevel.INFO,
            null);
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
