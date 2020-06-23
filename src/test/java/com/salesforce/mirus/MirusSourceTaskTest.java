/*
 *  Copyright (c) 2018, salesforce.com, inc.
 *  All rights reserved.
 *  SPDX-License-Identifier: BSD-3-Clause
 *  For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 *
 */

package com.salesforce.mirus;

import static org.hamcrest.CoreMatchers.hasItems;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;

import com.salesforce.mirus.config.SourceConfigDefinition;
import com.salesforce.mirus.config.TaskConfig.ReplayPolicy;
import com.salesforce.mirus.config.TaskConfigDefinition;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.data.ConnectSchema;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTaskContext;
import org.apache.kafka.connect.storage.OffsetStorageReader;
import org.junit.Before;
import org.junit.Test;

public class MirusSourceTaskTest {

  private static final String TOPIC = "topic1";
  private MirusSourceTask mirusSourceTask;
  private MockConsumer<byte[], byte[]> mockConsumer;

  @Before
  public void setUp() {
    mockConsumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
    mockConsumer.updatePartitions(
        TOPIC,
        Arrays.asList(
            new PartitionInfo(TOPIC, 0, null, null, null),
            new PartitionInfo(TOPIC, 1, null, null, null)));
    mirusSourceTask = new MirusSourceTask(consumerProperties -> mockConsumer);

    // Always return offset = 0
    SourceTaskContext context =
        new SourceTaskContext() {
          @Override
          public Map<String, String> configs() {
            return null;
          }

          @Override
          public OffsetStorageReader offsetStorageReader() {
            return new OffsetStorageReader() {
              @Override
              public <T> Map<String, Object> offset(Map<String, T> partition) {
                return new HashMap<>(MirusSourceTask.offsetMap(0L));
              }

              @Override
              public <T> Map<Map<String, T>, Map<String, Object>> offsets(
                  Collection<Map<String, T>> partitions) {
                return partitions.stream().collect(Collectors.toMap(p -> p, this::offset));
              }
            };
          }
        };
    mirusSourceTask.initialize(context);
    mirusSourceTask.start(mockTaskProperties());
  }

  private Map<String, String> mockTaskProperties() {
    Map<String, String> properties = new HashMap<>();
    List<TopicPartition> topicPartitionList = new ArrayList<>();
    topicPartitionList.add(new TopicPartition(TOPIC, 0));
    topicPartitionList.add(new TopicPartition(TOPIC, 1));
    properties.put(
        TaskConfigDefinition.PARTITION_LIST, TopicPartitionSerDe.toJson(topicPartitionList));
    properties.put(TaskConfigDefinition.REPLAY_POLICY, ReplayPolicy.FILTER.toString());
    properties.put(TaskConfigDefinition.REPLAY_WINDOW_RECORDS, "0");
    return properties;
  }

  @Test
  public void testSimplePollReturnsExpectedRecords() {
    mockConsumer.addRecord(new ConsumerRecord<>(TOPIC, 0, 0, new byte[] {}, new byte[] {}));
    mockConsumer.addRecord(new ConsumerRecord<>(TOPIC, 1, 0, new byte[] {}, new byte[] {}));
    List<SourceRecord> result = mirusSourceTask.poll();
    assertThat(result.size(), is(2));

    SourceRecord sourceRecord = result.get(0);
    assertThat(sourceRecord.headers().size(), is(0));
    assertThat(sourceRecord.kafkaPartition(), is(nullValue())); // Since partition matching is off
    assertThat(sourceRecord.keySchema().type(), is(ConnectSchema.BYTES_SCHEMA.type()));
    assertThat(sourceRecord.valueSchema().type(), is(ConnectSchema.BYTES_SCHEMA.type()));
    assertThat(sourceRecord.timestamp(), is(-1L)); // Since the source record has no timestamp
  }

  private ConsumerRecord<byte[], byte[]> newConsumerRecord(
      String topic, int partition, int offset, Long timestamp, Headers headers) {
    final Long checksum = 1234L;
    final byte[] key = "test-key".getBytes(StandardCharsets.UTF_8);
    final int serializedKeySize = key.length;
    final byte[] value = "test-value".getBytes(StandardCharsets.UTF_8);
    final int serializedValueSize = value.length;
    return new ConsumerRecord<>(
        topic,
        partition,
        offset,
        timestamp,
        TimestampType.CREATE_TIME,
        checksum,
        serializedKeySize,
        serializedValueSize,
        key,
        value,
        headers);
  }

  @Test
  public void testSourceRecordsWorksWithHeaders() {
    final String topic = "topica";
    final int partition = 0;
    final int offset = 123;
    final long timestamp = 314159;

    Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> records = new HashMap<>();
    Headers headers = new RecordHeaders();
    headers.add("h1", "v1".getBytes(StandardCharsets.UTF_8));
    headers.add("h2", "v2".getBytes(StandardCharsets.UTF_8));
    records.put(
        new TopicPartition(topic, partition),
        Collections.singletonList(newConsumerRecord(topic, partition, offset, timestamp, headers)));
    ConsumerRecords<byte[], byte[]> pollResult = new ConsumerRecords<>(records);

    List<SourceRecord> result = mirusSourceTask.sourceRecords(pollResult);

    assertThat(
        StreamSupport.stream(result.get(0).headers().spliterator(), false)
            .map(Header::key)
            .collect(Collectors.toList()),
        hasItems("h1", "h2"));
    assertThat(
        StreamSupport.stream(result.get(0).headers().spliterator(), false)
            .map(Header::value)
            .collect(Collectors.toList()),
        hasItems("v1".getBytes(StandardCharsets.UTF_8), "v2".getBytes(StandardCharsets.UTF_8)));
  }

  @Test
  public void testSourceRecordsWorksWithHeadersWithHeaderConverter() {
    final String topic = "topica";
    final int partition = 0;
    final int offset = 123;
    final long timestamp = 314159;

    Map<String, String> properties = mockTaskProperties();
    properties.put(
        SourceConfigDefinition.SOURCE_HEADER_CONVERTER.getKey(),
        "org.apache.kafka.connect.json.JsonConverter");

    mirusSourceTask.start(properties);

    Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> records = new HashMap<>();
    Headers headers = new RecordHeaders();
    headers.add(
        "h1",
        "{\"schema\": {\"type\": \"struct\",\"fields\": [{\"type\": \"string\",\"optional\": true,\"field\": \"version\"}],\"optional\": false},\"payload\": {\"version\": \"v1\"}}"
            .getBytes(StandardCharsets.UTF_8));
    headers.add(
        "h2",
        "{\"schema\": {\"type\": \"struct\",\"fields\": [{\"type\": \"string\",\"optional\": true,\"field\": \"version\"}],\"optional\": false},\"payload\": {\"version\": \"v2\"}}"
            .getBytes(StandardCharsets.UTF_8));
    records.put(
        new TopicPartition(topic, partition),
        Collections.singletonList(newConsumerRecord(topic, partition, offset, timestamp, headers)));
    ConsumerRecords<byte[], byte[]> pollResult = new ConsumerRecords<>(records);

    List<SourceRecord> result = mirusSourceTask.sourceRecords(pollResult);

    Iterator<Header> connectHeaders = result.get(0).headers().iterator();
    Header header1 = connectHeaders.next();
    assertThat(header1.key(), is("h1"));
    assertThat(header1.value(), instanceOf(Struct.class));
    Struct header1Value = (Struct) header1.value();
    assertThat(header1Value.getString("version"), is("v1"));

    Header header2 = connectHeaders.next();
    assertThat(header2.key(), is("h2"));
    assertThat(header2.value(), instanceOf(Struct.class));
    Struct header2Value = (Struct) header2.value();
    assertThat(header2Value.getString("version"), is("v2"));
  }

  @Test
  public void testSourceRecordsWorksWithNoHeaders() {
    final String topic = "topica";
    final int partition = 0;
    final int offset = 123;
    final long timestamp = 314159;

    Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> records = new HashMap<>();
    records.put(
        new TopicPartition(topic, partition),
        Collections.singletonList(
            newConsumerRecord(topic, partition, offset, timestamp, new RecordHeaders())));
    ConsumerRecords<byte[], byte[]> pollResult = new ConsumerRecords<>(records);

    List<SourceRecord> result = mirusSourceTask.sourceRecords(pollResult);

    assertThat(result.get(0).topic(), is(topic));
    assertThat(
        result.get(0).sourcePartition(),
        is(TopicPartitionSerDe.asMap(new TopicPartition(topic, partition))));
    assertThat(result.get(0).sourceOffset(), is(MirusSourceTask.offsetMap(offset + 1L)));
    assertThat(result.get(0).timestamp(), is(timestamp));
    assertThat(result.get(0).headers().size(), is(0));
  }

  @Test
  public void testJsonConverterRecord() {
    Map<String, String> properties = mockTaskProperties();
    properties.put(
        SourceConfigDefinition.SOURCE_KEY_CONVERTER.getKey(),
        "org.apache.kafka.connect.json.JsonConverter");
    properties.put(
        SourceConfigDefinition.SOURCE_VALUE_CONVERTER.getKey(),
        "org.apache.kafka.connect.json.JsonConverter");

    mirusSourceTask.start(properties);
    mockConsumer.addRecord(
        new ConsumerRecord<>(
            TOPIC,
            0,
            0,
            "{\"schema\": {\"type\": \"struct\",\"fields\": [{\"type\": \"string\",\"optional\": true,\"field\": \"id\"}],\"optional\": false},\"payload\": {\"id\": \"hiThereMirusKey\"}}"
                .getBytes(StandardCharsets.UTF_8),
            "{\"schema\": {\"type\": \"struct\",\"fields\": [{\"type\": \"string\",\"optional\": true,\"field\": \"id\"}],\"optional\": false},\"payload\": {\"id\": \"hiThereMirusValue\"}}"
                .getBytes(StandardCharsets.UTF_8)));

    List<SourceRecord> result = mirusSourceTask.poll();
    assertThat(result.size(), is(1));

    SourceRecord sourceRecord = result.get(0);
    assertThat(sourceRecord.headers().size(), is(0));
    assertThat(sourceRecord.kafkaPartition(), is(nullValue())); // Since partition matching is off
    assertThat(sourceRecord.keySchema().type(), is(Schema.Type.STRUCT));
    assertThat(sourceRecord.valueSchema().type(), is(Schema.Type.STRUCT));
    assertThat(sourceRecord.timestamp(), is(-1L)); // Since the source record has no timestamp
  }

  @Test
  public void testReplayFilterOnePartition() {

    mockConsumer.updateBeginningOffsets(Collections.singletonMap(new TopicPartition(TOPIC, 0), 0L));

    mockConsumer.addRecord(new ConsumerRecord<>(TOPIC, 0, 0, new byte[] {}, new byte[] {}));
    mockConsumer.addRecord(new ConsumerRecord<>(TOPIC, 0, 1, new byte[] {}, new byte[] {}));
    mockConsumer.addRecord(new ConsumerRecord<>(TOPIC, 0, 2, new byte[] {}, new byte[] {}));
    List<SourceRecord> result = mirusSourceTask.poll();
    assertThat(result.size(), is(3));

    // Simulate an offset reset
    mockConsumer.seekToBeginning(Collections.singletonList(new TopicPartition(TOPIC, 0)));

    mockConsumer.addRecord(new ConsumerRecord<>(TOPIC, 0, 0, new byte[] {}, new byte[] {}));
    mockConsumer.addRecord(new ConsumerRecord<>(TOPIC, 0, 1, new byte[] {}, new byte[] {}));
    mockConsumer.addRecord(new ConsumerRecord<>(TOPIC, 0, 2, new byte[] {}, new byte[] {}));
    mockConsumer.addRecord(new ConsumerRecord<>(TOPIC, 0, 3, new byte[] {}, new byte[] {}));
    result = mirusSourceTask.poll();

    assertThat(result.size(), is(1));
    assertThat(result.get(0).sourceOffset().get(MirusSourceTask.KEY_OFFSET), is(4L));
  }

  @Test
  public void testReplayFilterTwoPartitions() {

    Map<TopicPartition, Long> initialOffsets = new HashMap<>();
    initialOffsets.put(new TopicPartition(TOPIC, 0), 0L);
    initialOffsets.put(new TopicPartition(TOPIC, 1), 0L);

    mockConsumer.updateBeginningOffsets(initialOffsets);

    mockConsumer.addRecord(new ConsumerRecord<>(TOPIC, 0, 0, new byte[] {}, new byte[] {}));
    mockConsumer.addRecord(new ConsumerRecord<>(TOPIC, 0, 1, new byte[] {}, new byte[] {}));
    mockConsumer.addRecord(new ConsumerRecord<>(TOPIC, 0, 2, new byte[] {}, new byte[] {}));
    mockConsumer.addRecord(new ConsumerRecord<>(TOPIC, 1, 0, new byte[] {}, new byte[] {}));
    mockConsumer.addRecord(new ConsumerRecord<>(TOPIC, 1, 1, new byte[] {}, new byte[] {}));
    mockConsumer.addRecord(new ConsumerRecord<>(TOPIC, 1, 2, new byte[] {}, new byte[] {}));
    List<SourceRecord> result = mirusSourceTask.poll();
    assertThat(result.size(), is(6));

    // Simulate an offset reset on ONE partition
    mockConsumer.seekToBeginning(Collections.singletonList(new TopicPartition(TOPIC, 0)));

    mockConsumer.addRecord(new ConsumerRecord<>(TOPIC, 0, 0, new byte[] {}, new byte[] {}));
    mockConsumer.addRecord(new ConsumerRecord<>(TOPIC, 0, 1, new byte[] {}, new byte[] {}));
    mockConsumer.addRecord(new ConsumerRecord<>(TOPIC, 0, 2, new byte[] {}, new byte[] {}));
    mockConsumer.addRecord(new ConsumerRecord<>(TOPIC, 0, 3, new byte[] {}, new byte[] {}));
    mockConsumer.addRecord(new ConsumerRecord<>(TOPIC, 1, 3, new byte[] {}, new byte[] {}));
    result = mirusSourceTask.poll();

    assertThat(result.size(), is(2));
    assertThat(result.get(0).sourceOffset().get(MirusSourceTask.KEY_OFFSET), is(4L));
    assertThat(result.get(1).sourceOffset().get(MirusSourceTask.KEY_OFFSET), is(4L));
  }

  @Test
  public void testReplayFilterWindow() {

    Map<String, String> properties = mockTaskProperties();
    properties.put(TaskConfigDefinition.REPLAY_WINDOW_RECORDS, "2");
    mirusSourceTask.start(properties);

    mockConsumer.updateBeginningOffsets(Collections.singletonMap(new TopicPartition(TOPIC, 0), 0L));

    mockConsumer.addRecord(new ConsumerRecord<>(TOPIC, 0, 0, new byte[] {}, new byte[] {}));
    mockConsumer.addRecord(new ConsumerRecord<>(TOPIC, 0, 1, new byte[] {}, new byte[] {}));
    mockConsumer.addRecord(new ConsumerRecord<>(TOPIC, 0, 2, new byte[] {}, new byte[] {}));
    List<SourceRecord> result = mirusSourceTask.poll();
    assertThat(result.size(), is(3));

    // Simulate an offset reset
    mockConsumer.seekToBeginning(Collections.singletonList(new TopicPartition(TOPIC, 0)));

    mockConsumer.addRecord(new ConsumerRecord<>(TOPIC, 0, 0, new byte[] {}, new byte[] {}));
    mockConsumer.addRecord(new ConsumerRecord<>(TOPIC, 0, 1, new byte[] {}, new byte[] {}));
    mockConsumer.addRecord(new ConsumerRecord<>(TOPIC, 0, 2, new byte[] {}, new byte[] {}));
    mockConsumer.addRecord(new ConsumerRecord<>(TOPIC, 0, 3, new byte[] {}, new byte[] {}));
    result = mirusSourceTask.poll();

    assertThat(result.size(), is(3));
    assertThat(result.get(0).sourceOffset().get(MirusSourceTask.KEY_OFFSET), is(2L));
  }
}
