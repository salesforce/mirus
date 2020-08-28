/*
 *  Copyright (c) 2018, salesforce.com, inc.
 *  All rights reserved.
 *  SPDX-License-Identifier: BSD-3-Clause
 *  For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 *
 */

package com.salesforce.mirus;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.apache.kafka.connect.storage.Converter;
import org.apache.kafka.connect.storage.HeaderConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.salesforce.mirus.config.TaskConfig;
import com.salesforce.mirus.config.TaskConfig.ReplayPolicy;

interface ConsumerFactory {
  Consumer<byte[], byte[]> newConsumer(Map<String, Object> consumerProperties);
}

/**
 * MirusSourceTask is a {@link SourceTask} implementation that knows how to mirror a set of
 * partitions between two Kafka clusters. On startup it instantiates a new {@link KafkaConsumer}
 * instance using the "consumer.*" properties in the Source configuration object {@link
 * com.salesforce.mirus.config.SourceConfig}. It then seeks the current offsets for each partition
 * and starting polling.
 *
 * <p>Optionally supports partition id matching between source and destination topics.
 */
public class MirusSourceTask extends SourceTask {

  private static final Logger logger = LoggerFactory.getLogger(MirusSourceTask.class);

  static final String KEY_OFFSET = "offset";

  private final ConsumerFactory consumerFactory;

  private Map<String, Object> consumerProperties;
  private long consumerPollTimeoutMillis;
  private String destinationTopicNamePrefix;
  private String destinationTopicNameSuffix;
  private Consumer<byte[], byte[]> consumer;
  private boolean enablePartitionMatching = false;

  private Converter keyConverter;
  private Converter valueConverter;
  private HeaderConverter headerConverter;
  private ReplayPolicy replayPolicy;
  private long replayWindowRecords;

  private final Map<TopicPartition, Long> latestOffsetMap = new HashMap<>();
  private final Set<TopicPartition> loggingFlags = new HashSet<>();

  protected AtomicBoolean shutDown = new AtomicBoolean(false);

  @SuppressWarnings("unused")
  public MirusSourceTask() {
    this(KafkaConsumer::new);
  }

  MirusSourceTask(ConsumerFactory consumerFactory) {
    this.consumerFactory = consumerFactory;
  }

  public static Map<String, Long> offsetMap(Long offset) {
    return Collections.singletonMap(KEY_OFFSET, offset);
  }

  @Override
  public String version() {
    return new MirusSourceConnector().version();
  }

  @Override
  public void start(Map<String, String> properties) {

    logger.debug("Task starting with properties: {}", properties);
    Thread.currentThread().setName("MirusSourceTask");

    TaskConfig config = new TaskConfig(properties);
    this.consumerProperties = config.getConsumerProperties();

    // Get properties
    this.consumerPollTimeoutMillis = config.getConsumerPollTimeout();
    this.destinationTopicNamePrefix = config.getDestinationTopicNamePrefix();
    this.destinationTopicNameSuffix = config.getDestinationTopicNameSuffix();
    this.enablePartitionMatching = config.getEnablePartitionMatching();

    this.keyConverter = config.getKeyConverter();
    this.valueConverter = config.getValueConverter();
    this.headerConverter = config.getHeaderConverter();
    this.replayPolicy = config.getReplayPolicy();
    this.replayWindowRecords = config.getReplayWindowRecords();

    logger.debug("Task starting with partitions: {}", config.getInternalTaskPartitions());

    this.consumer = consumerFactory.newConsumer(consumerProperties);
    List<TopicPartition> partitionIds =
        TopicPartitionSerDe.parseTopicPartitionList(config.getInternalTaskPartitions());
    this.consumer.assign(partitionIds);
    seekToOffsets(partitionIds);
    shutDown.set(false);
  }

  @Override
  public void stop() {
    if (shutDown != null) {
      shutDown.set(true);
      consumer.wakeup();
    }
  }

  protected void shutDownTask() {
    logger.debug("Task shutting down");
    consumer.close();
  }

  private void seekToOffsets(List<TopicPartition> partitionIds) {
    Collection<Map<String, Object>> partitionMaps =
        partitionIds.stream().map(TopicPartitionSerDe::asMap).collect(Collectors.toList());
    Map<Map<String, Object>, Map<String, Object>> offsets =
        context.offsetStorageReader().offsets(partitionMaps);
    if (offsets == null) {
      return;
    }
    logger.debug("Seeking to partition offsets: {}", offsets);
    offsets.forEach(
        (partitionMap, offsetMap) -> {
          TopicPartition tp =
              new TopicPartition(
                  (String) partitionMap.get(TopicPartitionSerDe.KEY_TOPIC),
                  (int) partitionMap.get(TopicPartitionSerDe.KEY_PARTITION));

          // check if offset has been set to null, i.e. tombstone record
          // or if no offset record at all
          if (offsetMap == null || offsetMap.get(KEY_OFFSET) == null) {
            // No offsets available so seek to start or end
            // (need to do this explicitly if manually seeking).
            String offsetReset = (String) consumerProperties.get("auto.offset.reset");
            if ("latest".equalsIgnoreCase(offsetReset)) {
              logger.trace("Seeking to end");
              consumer.seekToEnd(Collections.singletonList(tp));
            } else {
              logger.trace("Seeking to beginning");
              consumer.seekToBeginning(Collections.singletonList(tp));
            }
            if (logger.isTraceEnabled()) {
              long pos = consumer.position(tp);
              logger.trace("{} at position {}", tp, pos);
            }
          } else {
            consumer.seek(tp, (Long) offsetMap.get(KEY_OFFSET));
          }
        });
  }

  @Override
  public List<SourceRecord> poll() {

    try {
      logger.trace("Calling poll");
      ConsumerRecords<byte[], byte[]> result = consumer.poll(consumerPollTimeoutMillis);
      logger.trace("Got {} records", result.count());
      if (!result.isEmpty()) {
        return sourceRecords(result);
      } else {
        return Collections.emptyList();
      }
    } catch (WakeupException e) {
      // Ignore exception iff shutting down thread.
      if (!shutDown.get()) throw e;
    }

    shutDownTask();
    return Collections.emptyList();
  }

  List<SourceRecord> sourceRecords(ConsumerRecords<byte[], byte[]> pollResult) {
    List<SourceRecord> sourceRecords = new ArrayList<>(pollResult.count());
    pollResult.forEach(
        consumerRecord -> {
          if (replayPolicy == ReplayPolicy.FILTER && !isSkippedRecord(consumerRecord)) {
            sourceRecords.add(toSourceRecord(consumerRecord));
          }
        });
    return sourceRecords;
  }

  private boolean isSkippedRecord(ConsumerRecord<byte[], byte[]> consumerRecord) {
    TopicPartition topicPartition = new TopicPartition(consumerRecord.topic(), consumerRecord.partition());
    long sourceOffset = consumerRecord.offset();
    Long latestOffset = latestOffsetMap.get(topicPartition);
    // Skip any record that has already been handled by this task
    if (latestOffset != null && sourceOffset <= (latestOffset - replayWindowRecords)) {
      maybeLogSkippedRecord(topicPartition, sourceOffset, latestOffset);
      return true;
    } else {
      latestOffsetMap.put(topicPartition, sourceOffset);
    }
    return false;
  }

  private void maybeLogSkippedRecord(TopicPartition topicPartition, long sourceOffset, long latestOffset) {
    if(!loggingFlags.contains(topicPartition)) {
      logger.info("Skipping record with topic-partition={}, offset={}. Latest previously recorded offset={}. "
        + "This log statement is recorded once per task instance per topic-partition.",
        topicPartition, sourceOffset, latestOffset);
      loggingFlags.add(topicPartition);
    }

  }

  private SourceRecord toSourceRecord(ConsumerRecord<byte[], byte[]> consumerRecord) {
    Map<String, Object> sourcePartition =
        TopicPartitionSerDe.asMap(
            new TopicPartition(consumerRecord.topic(), consumerRecord.partition()));

    String topic = destinationTopicNamePrefix + consumerRecord.topic() + destinationTopicNameSuffix;

    ConnectHeaders connectHeaders = new ConnectHeaders();
    Headers sourceHeaders = consumerRecord.headers();
    if (sourceHeaders != null) {
      sourceHeaders.forEach(
          header ->
              connectHeaders.add(
                  header.key(),
                  this.headerConverter.toConnectHeader(topic, header.key(), header.value())));
    }

    SchemaAndValue keyAndSchema = this.keyConverter.toConnectData(topic, consumerRecord.key());

    SchemaAndValue valueAndSchema =
        this.valueConverter.toConnectData(topic, consumerRecord.value());

    /*
     * NOTE: By adding one to the offset here we are following the Kafka convention that the
     * committed offset should always be the offset of the *next* message that your application will
     * read.
     */
    return new SourceRecord(
        sourcePartition,
        offsetMap(consumerRecord.offset() + 1),
        topic,
        enablePartitionMatching ? consumerRecord.partition() : null,
        keyAndSchema.schema(),
        keyAndSchema.value(),
        valueAndSchema.schema(),
        valueAndSchema.value(),
        consumerRecord.timestamp(),
        connectHeaders);
  }
}
