/*
 *  Copyright (c) 2018, salesforce.com, inc.
 *  All rights reserved.
 *  SPDX-License-Identifier: BSD-3-Clause
 *  For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 *
 */

package com.salesforce.mirus.offsets;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.stream.Stream;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.connect.runtime.WorkerConfig;
import org.apache.kafka.connect.runtime.distributed.DistributedConfig;
import org.apache.kafka.connect.storage.Converter;
import org.apache.kafka.connect.util.Callback;
import org.apache.kafka.connect.util.ConvertingFutureCallback;
import org.apache.kafka.connect.util.KafkaBasedLog;

/**
 * Grabs all records from a Kafka Connect offset storage topic. Wraps KafkaBasedLog and requires the
 * user to call start() and stop() in a similar way.
 */
class OffsetFetcher {

  private final KafkaBasedLog<byte[], byte[]> offsetLog;
  private final Map<ByteBuffer, ByteBuffer> data = new HashMap<>();
  private final Converter internalConverter;

  private boolean started = false;

  OffsetFetcher(final WorkerConfig config, Converter internalConverter) {
    String topic = config.getString(DistributedConfig.OFFSET_STORAGE_TOPIC_CONFIG);
    if ("".equals(topic)) {
      throw new ConfigException("Offset storage topic must be specified");
    }

    Map<String, Object> producerProps = new HashMap<>(config.originals());
    producerProps.put(
        ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
    producerProps.put(
        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
    producerProps.put(ProducerConfig.RETRIES_CONFIG, Integer.MAX_VALUE);

    Map<String, Object> consumerProps = new HashMap<>(config.originals());
    consumerProps.put(
        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
    consumerProps.put(
        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());

    Callback<ConsumerRecord<byte[], byte[]>> consumedCallback =
        (error, record) -> {
          ByteBuffer key = record.key() != null ? ByteBuffer.wrap(record.key()) : null;
          ByteBuffer value = record.value() != null ? ByteBuffer.wrap(record.value()) : null;
          data.put(key, value);
        };
    this.offsetLog =
        new KafkaBasedLog<>(
            topic, producerProps, consumerProps, consumedCallback, Time.SYSTEM, null);
    this.internalConverter = internalConverter;
  }

  /** Get a map containing the current state of all records in the offset log. */
  private Map<ByteBuffer, ByteBuffer> get() {
    ConvertingFutureCallback<Void, Map<ByteBuffer, ByteBuffer>> future =
        new ConvertingFutureCallback<Void, Map<ByteBuffer, ByteBuffer>>(null) {
          @Override
          public Map<ByteBuffer, ByteBuffer> convert(Void result) {
            return data;
          }
        };
    offsetLog.readToEnd(future);
    try {
      return future.get();
    } catch (InterruptedException | ExecutionException e) {
      throw new RuntimeException("Unable to fetch offsets from Kafka", e);
    }
  }

  void start() {
    offsetLog.start();
    started = true;
  }

  void stop() {
    offsetLog.stop();
    started = false;
  }

  Stream<OffsetInfo> readOffsets() {
    if (!started) {
      throw new RuntimeException("OffsetFetcher has not been started");
    }
    Map<ByteBuffer, ByteBuffer> offsets = get();
    return offsets.entrySet().stream().map(this::toOffsetInfo);
  }

  @SuppressWarnings("unchecked")
  private OffsetInfo toOffsetInfo(Map.Entry<ByteBuffer, ByteBuffer> offsetEntry) {
    Object kafkaConnectOffsetKey =
        internalConverter.toConnectData(null, offsetEntry.getKey().array()).value();
    // Deserialize the internal Mirus offset format
    List<Object> keyList = (List) kafkaConnectOffsetKey;
    Map<String, Object> parts = (Map) keyList.get(1);

    ByteBuffer val = offsetEntry.getValue();
    Long offset = null;

    // Handle null-valued records, i.e. tombstone messages
    if (val != null) {
      Object kafkaConnectOffsetValue = internalConverter.toConnectData(null, val.array()).value();
      Map<String, Object> valueMap = (Map) kafkaConnectOffsetValue;
      offset = (Long) valueMap.get("offset");
    }

    return new OffsetInfo(
        (String) keyList.get(0),
        (String) parts.get("topic"),
        ((Long) parts.get("partition")),
        offset);
  }
}
