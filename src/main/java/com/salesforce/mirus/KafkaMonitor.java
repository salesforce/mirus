/*
 *  Copyright (c) 2018, salesforce.com, inc.
 *  All rights reserved.
 *  SPDX-License-Identifier: BSD-3-Clause
 *  For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 *
 */

package com.salesforce.mirus;

import com.salesforce.mirus.config.SourceConfig;
import com.salesforce.mirus.metrics.MissingPartitionsJmxReporter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.RetriableException;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.connect.connector.ConnectorContext;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.transforms.RegexRouter;
import org.apache.kafka.connect.transforms.Transformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The KafkaMonitor thread is started by {@link MirusSourceConnector} and polls the source and
 * destination clusters to maintain an up-to-date list of partitions eligible for mirroring. When a
 * change is detected the thread requests that tasks for this source are reconfigured. Partitions
 * that match the configured whitelist are validated to ensure they exist in both source and the
 * destination cluster. Validation supports topic re-routing with the RegexRouter Transformation,
 * but no other topic re-routing is supported. Validation may be disabled by setting the
 * enable.destination.topic.checking config option to false.
 *
 * <p>MirusSourceConnector also uses KafkaMonitor for task assignment. A round-robin style algorithm
 * is used to assign partitions to SourceTask instances.
 */
class KafkaMonitor implements Runnable {

  private static final Logger logger = LoggerFactory.getLogger(KafkaMonitor.class);
  private static final int[] BACKOFF_WAIT_SECONDS = {0, 1, 2, 4, 8, 16, 32, 64};

  private final ConnectorContext context;
  private final List<String> topicsWhitelist;
  private final Pattern topicsRegexPattern;
  private final CountDownLatch shutDownLatch = new CountDownLatch(1);
  private final Consumer<byte[], byte[]> sourceConsumer;
  private final Consumer<byte[], byte[]> destinationConsumer;
  private final Long monitorPollWaitMs;
  private final TaskConfigBuilder taskConfigBuilder;
  private final SourcePartitionValidator.MatchingStrategy validationStrategy;
  private final MissingPartitionsJmxReporter missingPartsJmxReporter =
      new MissingPartitionsJmxReporter();
  private final List<Transformation<SourceRecord>> routers;
  private final boolean topicCheckingEnabled;

  // The current list of partitions to replicate.
  private volatile List<TopicPartition> topicPartitionList;

  KafkaMonitor(ConnectorContext context, SourceConfig config, TaskConfigBuilder taskConfigBuilder) {
    this(
        context,
        config,
        newSourceConsumer(config),
        newDestinationConsumer(config),
        taskConfigBuilder);
  }

  KafkaMonitor(
      ConnectorContext context,
      SourceConfig config,
      Consumer<byte[], byte[]> sourceConsumer,
      Consumer<byte[], byte[]> destinationConsumer,
      TaskConfigBuilder taskConfigBuilder) {
    this.context = context;
    this.topicsWhitelist = config.getTopicsWhitelist();
    this.monitorPollWaitMs = config.getMonitorPollWaitMs();
    this.topicsRegexPattern = Pattern.compile(config.getTopicsRegex());
    this.sourceConsumer = sourceConsumer;
    this.destinationConsumer = destinationConsumer;
    if (topicsWhitelist.isEmpty() && config.getTopicsRegex().isEmpty()) {
      logger.warn("No whitelist configured");
    }
    this.taskConfigBuilder = taskConfigBuilder;
    this.validationStrategy =
        config.getEnablePartitionMatching()
            ? SourcePartitionValidator.MatchingStrategy.PARTITION
            : SourcePartitionValidator.MatchingStrategy.TOPIC;
    this.topicCheckingEnabled = config.getTopicCheckingEnabled();
    this.routers = this.validateTransformations(config.transformations());
  }

  private List<Transformation<SourceRecord>> validateTransformations(
      List<Transformation<SourceRecord>> transformations) {
    List<Transformation<SourceRecord>> regexRouters = new ArrayList<>();

    // No need to validate transforms if we're not checking destination partitions
    if (this.topicCheckingEnabled) {
      for (Transformation<SourceRecord> transform : transformations) {
        String transformName = transform.getClass().getSimpleName();
        if (transform instanceof RegexRouter) {
          regexRouters.add(transform);
          // Slightly awkward check to see if any other routing transforms are configured
        } else if (transformName.contains("Router")) {
          throw new IllegalArgumentException(
              String.format(
                  "Unsupported Router Transformation %s found."
                      + " To use it, please disable destination topic checking by setting 'enable.destination.topic.checking' to false.",
                  transformName));
        } else {
          logger.debug("Ignoring non-routing Transformation {}", transformName);
        }
      }
    }
    return regexRouters;
  }

  private String applyRoutersToTopic(String topic) {
    TopicPartition topicPartition = new TopicPartition(topic, 0);
    Map<String, Object> sourcePartition = TopicPartitionSerDe.asMap(topicPartition);
    SourceRecord record =
        new SourceRecord(
            sourcePartition,
            null,
            topicPartition.topic(),
            topicPartition.partition(),
            Schema.BYTES_SCHEMA,
            null,
            Schema.OPTIONAL_BYTES_SCHEMA,
            null);
    for (Transformation<SourceRecord> transform : this.routers) {
      record = transform.apply(record);
    }
    return record.topic();
  }

  private static Consumer<byte[], byte[]> newSourceConsumer(SourceConfig config) {
    Map<String, Object> consumerProperties = config.getConsumerProperties();

    // The "monitor1" client id suffix is used to keep JMX bean names distinct
    consumerProperties.computeIfPresent(
        CommonClientConfigs.CLIENT_ID_CONFIG, (k, v) -> v + "monitor1");
    return new KafkaConsumer<>(consumerProperties);
  }

  private static Consumer<byte[], byte[]> newDestinationConsumer(SourceConfig config) {
    Map<String, Object> consumerProperties = config.getConsumerProperties();

    // The "monitor2" client id suffix is used to keep JMX bean names distinct
    consumerProperties.computeIfPresent(
        CommonClientConfigs.CLIENT_ID_CONFIG, (k, v) -> v + "monitor2");
    consumerProperties.put(
        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, config.getDestinationBootstrapServers());
    return new KafkaConsumer<>(consumerProperties);
  }

  @Override
  public void run() {
    int consecutiveRetriableErrors = 0;
    while (true) {
      try {
        // Do a fast shutdown check first thing in case we're in an exponential backoff retry loop,
        // which will never hit the poll wait below
        if (shutDownLatch.await(0, TimeUnit.MILLISECONDS)) {
          logger.debug("Exiting KafkaMonitor");
          return;
        }
        if (this.topicPartitionList == null) {
          // Need to initialize here to prevent the constructor hanging on startup if the
          // source cluster is unavailable.
          this.topicPartitionList = fetchTopicPartitionList();
        }

        if (partitionsChanged()) {
          logger.info("Source partition change detected.  Requesting task reconfiguration.");
          this.context.requestTaskReconfiguration();
        }

        if (shutDownLatch.await(monitorPollWaitMs, TimeUnit.MILLISECONDS)) {
          logger.debug("Exiting KafkaMonitor");
          return;
        }
        consecutiveRetriableErrors = 0;
      } catch (WakeupException | InterruptedException e) {
        // Assume we've been woken or interrupted to shutdown, so continue on to checking the
        // shutDownLatch next iteration.
        logger.debug("KafkaMonitor woken up, checking if shutdown requested...");
      } catch (RetriableException e) {
        consecutiveRetriableErrors += 1;
        logger.warn(
            "Retriable exception encountered ({} consecutive), continuing processing...",
            consecutiveRetriableErrors,
            e);
        exponentialBackoffWait(consecutiveRetriableErrors);
      } catch (Exception e) {
        logger.error("Raising exception to connect runtime", e);
        context.raiseError(e);
      }
    }
  }

  private void exponentialBackoffWait(int numErrors) {
    int secondsToWait =
        BACKOFF_WAIT_SECONDS[Math.min(Math.max(numErrors - 1, 0), BACKOFF_WAIT_SECONDS.length - 1)];
    if (secondsToWait > 0) {
      try {
        TimeUnit.SECONDS.sleep(secondsToWait);
      } catch (InterruptedException e) {
        logger.debug("Interrupted while sleeping due to retriable errors, resuming...");
      }
    }
  }

  /** Returns true if the subscribed partition list has changed and we need a rebalance. */
  boolean partitionsChanged() {
    List<TopicPartition> oldPartitionInfoList = topicPartitionList;
    topicPartitionList = fetchTopicPartitionList();
    logger.trace("Detected {} matching partitions", topicPartitionList.size());

    if (oldPartitionInfoList == null) {
      // List not initialized yet, so don't trigger rebalance
      return false;
    }

    // Note: These two lists are already sorted
    boolean partitionsChanged = !topicPartitionList.equals(oldPartitionInfoList);
    if (partitionsChanged) {
      logger.debug(
          "Replicated partition set change. Triggering re-balance with {} partitions",
          topicPartitionList.size());
    } else {
      logger.debug("No change to replicated partition set");
    }
    return partitionsChanged;
  }

  private List<TopicPartition> fetchMatchingPartitions(Consumer<byte[], byte[]> consumer) {
    return consumer
        .listTopics()
        .entrySet()
        .stream()
        .filter(
            e ->
                topicsWhitelist.contains(e.getKey())
                    || topicsRegexPattern.matcher(e.getKey()).matches())
        .flatMap(e -> e.getValue().stream())
        .map(partitionInfo -> new TopicPartition(partitionInfo.topic(), partitionInfo.partition()))
        .collect(Collectors.toList());
  }

  private List<TopicPartition> fetchTopicPartitionList() {
    logger.trace(
        "Fetching partition details for topicsWhitelist: {}, topicsRegex: {}",
        topicsWhitelist,
        topicsRegexPattern.toString());

    List<TopicPartition> sourcePartitionList;
    synchronized (sourceConsumer) {
      sourcePartitionList = fetchMatchingPartitions(sourceConsumer);
    }

    List<TopicPartition> result;
    if (this.topicCheckingEnabled) {
      result = getDestinationAvailablePartitions(sourcePartitionList);
    } else {
      result = sourcePartitionList;
    }

    // Sort the result for order-independent comparison
    result.sort(Comparator.comparing(tp -> tp.topic() + tp.partition()));
    return result;
  }

  private List<TopicPartition> getDestinationAvailablePartitions(
      List<TopicPartition> sourcePartitionList) {
    SourcePartitionValidator sourcePartitionValidator =
        new SourcePartitionValidator(
            destinationConsumer, validationStrategy, this::applyRoutersToTopic);

    // Split the source partition list into those contained in the destination, and those
    // missing. Using toCollection(ArrayList::new) to guarantee we can sort successfully.
    Map<Boolean, List<TopicPartition>> partitionedSourceIds =
        sourcePartitionList
            .stream()
            .collect(
                Collectors.partitioningBy(
                    sourcePartitionValidator::isHealthy, Collectors.toCollection(ArrayList::new)));

    List<TopicPartition> missingPartitions = partitionedSourceIds.get(false);
    if (!missingPartitions.isEmpty()) {
      logger.warn(
          "Missing target partitions {}. Topics may need to be added to the target cluster.",
          missingPartitions);
    }

    missingPartsJmxReporter.recordMetric(missingPartitions.size());

    List<TopicPartition> result = partitionedSourceIds.get(true);
    return result;
  }

  /**
   * Determines the MirusSourceConnector task configuration, including partition assignment. This is
   * called from the main SourceConnector thread.
   *
   * @param maxTasks The maximum number of tasks this SourceConnector is configured to run in
   *     parallel across the cluster.
   */
  List<Map<String, String>> taskConfigs(int maxTasks) {
    logger.debug("Called taskConfigs maxTasks {}", maxTasks);

    // No need to synchronize while waiting for initialization
    long tStart = System.currentTimeMillis();
    while (this.topicPartitionList == null && (System.currentTimeMillis() - tStart) < 60000) {
      try {
        Thread.sleep(3000);
        // List will normally be initialized within this time
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }

    synchronized (this) {
      if (topicPartitionList == null) {
        // Timed out waiting for initialization. Initialize with empty list so we can detect
        // changes.
        logger.error("Topic partition list not initialized. Falling back on empty list.");
        this.topicPartitionList = new ArrayList<>();
      }
      if (topicPartitionList.isEmpty()) {
        return Collections.emptyList();
      }
      return taskConfigBuilder.fromPartitionList(maxTasks, topicPartitionList);
    }
  }

  void stop() {
    missingPartsJmxReporter.recordMetric(0);
    shutDownLatch.countDown();
    sourceConsumer.wakeup();
    destinationConsumer.wakeup();
    synchronized (sourceConsumer) {
      sourceConsumer.close();
    }
    synchronized (destinationConsumer) {
      destinationConsumer.close();
    }
  }
}
