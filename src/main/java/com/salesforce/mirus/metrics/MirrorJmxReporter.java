package com.salesforce.mirus.metrics;

import com.salesforce.mirus.MirusSourceConnector;
import org.apache.kafka.common.MetricNameTemplate;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

public class MirrorJmxReporter extends AbstractMirusJmxReporter {

    private static final Logger logger = LoggerFactory.getLogger(MirrorJmxReporter.class);

    private static MirrorJmxReporter instance = null;

    private static final String SOURCE_CONNECTOR_GROUP = MirusSourceConnector.class.getSimpleName();

    private static final Set<String> TOPIC_TAGS = new HashSet<>(Collections.singletonList("topic"));

    private static final MetricNameTemplate REPLICATION_LATENCY = new MetricNameTemplate(
            "replication-latency-ms", SOURCE_CONNECTOR_GROUP,
            "Time it takes records to replicate from source to target cluster.", TOPIC_TAGS);
    private static final MetricNameTemplate REPLICATION_LATENCY_MAX = new MetricNameTemplate(
            "replication-latency-ms-max", SOURCE_CONNECTOR_GROUP,
            "Max time it takes records to replicate from source to target cluster.", TOPIC_TAGS);
    private static final MetricNameTemplate REPLICATION_LATENCY_MIN = new MetricNameTemplate(
            "replication-latency-ms-min", SOURCE_CONNECTOR_GROUP,
            "Min time it takes records to replicate from source to target cluster.", TOPIC_TAGS);
    private static final MetricNameTemplate REPLICATION_LATENCY_AVG = new MetricNameTemplate(
            "replication-latency-ms-avg", SOURCE_CONNECTOR_GROUP,
            "Average time it takes records to replicate from source to target cluster.", TOPIC_TAGS);

    // Map of topics to their metric objects
    private final Map<String, Sensor> topicSensors;
    private final Set<TopicPartition> topicPartitionSet;

    private MirrorJmxReporter() {
        super(new Metrics());
        metrics.sensor("replication-latency");

        topicSensors = new HashMap<>();
        topicPartitionSet = new HashSet<>();

        logger.info("Initialized MirrorJMXReporter");
    }
    public synchronized static MirrorJmxReporter getInstance() {
        if (null == instance) {
            instance = new MirrorJmxReporter();
        }
        return instance;
    }

    /**
     * Add a list of partitions to record the latency of.
     * @param topicPartitions
     */
    public synchronized void addTopics(List<TopicPartition> topicPartitions) {
        topicSensors.putAll(
                topicPartitions
                        .stream()
                        .map(TopicPartition::topic)
                        .distinct()
                        .filter(topic -> !topicSensors.containsKey(topic))
                        .collect(Collectors.toMap(topic -> topic, this::createTopicSensor))
        );
        topicPartitionSet.addAll(topicPartitions);
    }

    /**
     * Remove a list of partitions from the metrics, this is involved since we want to record
     * if a single partition of a topic is held by this worker.
     * @param topicPartitions
     */
    public synchronized void removeTopics(List<TopicPartition> topicPartitions) {
        // We want to remove all topics that are passed in topicPartitions
        // However, we need to be smart since the topic may still be "owned" by a different partition.

        topicPartitionSet.removeAll(topicPartitions);

        // Get all the distinct topic names that should still exist in the sensors.
        // Then subtract that set from topics to remove.
        // We don't need calls to "distinct" since we are operating with sets of strings.
        Set<String> currentTopics = topicPartitionSet
                .stream()
                .map(TopicPartition::topic)
                .collect(Collectors.toSet());

        Set<String> topicsToRemove = topicPartitions
                .stream()
                .map(TopicPartition::topic)
                .filter(topic -> !currentTopics.contains(topic))
                .collect(Collectors.toSet());


        topicsToRemove.forEach(
                topic -> {
                    metrics.removeSensor(replicationLatencySensorName(topic));
                    topicSensors.remove(topic);
                }
        );
    }


    public synchronized void recordMirrorLatency(String topic, long millis) {
        Sensor sensor = topicSensors.get(topic);
        if (sensor != null) {
            sensor.record((double) millis);
        }
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

    private String replicationLatencySensorName(String topic) {
        return topic + "-" + "replication-latency";
    }
}
