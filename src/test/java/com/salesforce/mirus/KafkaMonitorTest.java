/*
 *  Copyright (c) 2018, salesforce.com, inc.
 *  All rights reserved.
 *  SPDX-License-Identifier: BSD-3-Clause
 *  For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 *
 */

package com.salesforce.mirus;

import static org.hamcrest.CoreMatchers.*;
import static org.hamcrest.Matchers.contains;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;

import com.salesforce.mirus.assignment.RoundRobinTaskAssignor;
import com.salesforce.mirus.config.SourceConfig;
import com.salesforce.mirus.config.SourceConfigDefinition;
import com.salesforce.mirus.config.TaskConfig;
import com.salesforce.mirus.config.TaskConfigDefinition;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.connector.ConnectorContext;
import org.junit.Before;
import org.junit.Test;

public class KafkaMonitorTest {

  private KafkaMonitor kafkaMonitor;
  private MockConsumer<byte[], byte[]> mockSourceConsumer;
  private MockConsumer<byte[], byte[]> mockDestinationConsumer;

  @Before
  public void setUp() {
    Map<String, String> properties = new HashMap<>();
    properties.put(SourceConfigDefinition.TOPICS_REGEX.getKey(), "topic.*");
    properties.put(TaskConfigDefinition.CONSUMER_CLIENT_ID, "testId");
    properties.put("name", "mockKafkaMonitor");
    properties.put("connector.class", "com.salesforce.mirus.MirusSourceConnector");
    SourceConfig config = new SourceConfig(properties);
    this.mockSourceConsumer = mockSourceConsumer();
    this.mockDestinationConsumer = mockDestinationConsumer();
    TaskConfigBuilder taskConfigBuilder =
        new TaskConfigBuilder(new RoundRobinTaskAssignor(), config);
    kafkaMonitor =
        new KafkaMonitor(
            mock(ConnectorContext.class),
            config,
            mockSourceConsumer,
            mockDestinationConsumer,
            taskConfigBuilder);
  }

  private void updateMockPartitions(
      MockConsumer<byte[], byte[]> mockConsumer, String topicName, int numPartitions) {
    List<PartitionInfo> partitionInfoList = new ArrayList<>();
    for (int i = 0; i < numPartitions; i++) {
      partitionInfoList.add(new PartitionInfo(topicName, i, null, null, null));
    }
    mockConsumer.updatePartitions(topicName, partitionInfoList);
  }

  private MockConsumer<byte[], byte[]> mockSourceConsumer() {
    MockConsumer<byte[], byte[]> mockConsumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
    updateMockPartitions(mockConsumer, "topic1", 2);
    updateMockPartitions(mockConsumer, "topic2", 1);
    updateMockPartitions(mockConsumer, "topic3", 1);
    updateMockPartitions(mockConsumer, "topic4", 1);
    updateMockPartitions(mockConsumer, "topic5", 1);
    updateMockPartitions(mockConsumer, "reroute.outgoing", 1);
    return mockConsumer;
  }

  private MockConsumer<byte[], byte[]> mockDestinationConsumer() {
    // Topic 5 is NOT present in destination
    MockConsumer<byte[], byte[]> mockConsumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
    updateMockPartitions(mockConsumer, "topic1", 2);
    updateMockPartitions(mockConsumer, "topic2", 1);
    updateMockPartitions(mockConsumer, "topic3", 1);
    updateMockPartitions(mockConsumer, "topic4", 1);
    updateMockPartitions(mockConsumer, "reroute.incoming", 1);
    return mockConsumer;
  }

  private List<TopicPartition> assignedTopicPartitionsFromTaskConfigs(
      List<Map<String, String>> taskConfigs) {
    return taskConfigs
        .stream()
        // Get the string value of partition assignment for each task.
        .map(i -> new TaskConfig(i).getInternalTaskPartitions())
        // Then parse that into TopicPartitions and flatten into a single list.
        .flatMap(i -> TopicPartitionSerDe.parseTopicPartitionList(i).stream())
        .collect(Collectors.toList());
  }

  @Test
  public void shouldOnlyAssignPartitionsPresentInDestination() {
    kafkaMonitor.partitionsChanged();
    List<Map<String, String>> result = kafkaMonitor.taskConfigs(3);

    List<TopicPartition> partitions = assignedTopicPartitionsFromTaskConfigs(result);

    // Topic 5 is not present so should be removed from the partition list.
    assertThat(partitions, not(hasItem(new TopicPartition("topic5", 0))));

    // Only 5 partitions should be assigned, as only 5 partitions are valid.
    assertThat(partitions.size(), is(5));
  }

  @Test
  public void shouldUpdateTaskConfigWhenPartitionsAddedToDestination() {
    assertThat(kafkaMonitor.partitionsChanged(), is(false));

    // Make sure 5 of 6 source partitions are assigned (one is missing from destination).
    assertThat(kafkaMonitor.taskConfigs(50).size(), is(5));

    // Add missing partition to destination cluster.
    updateMockPartitions(mockDestinationConsumer, "topic5", 1);
    assertThat(kafkaMonitor.partitionsChanged(), is(true));

    // Make sure all 6 partitions are now assigned.
    List<Map<String, String>> result = kafkaMonitor.taskConfigs(50);
    assertThat(result.size(), is(6));
  }

  @Test
  public void shouldNotReBalanceIfOnlySourcePartitionOrderHasChanged() {
    // Change order of partitions in topic 1.
    mockSourceConsumer.updatePartitions(
        "topic1",
        Arrays.asList(
            new PartitionInfo("topic1", 1, null, null, null),
            new PartitionInfo("topic1", 0, null, null, null)));
    assertThat(kafkaMonitor.partitionsChanged(), is(false));
  }

  @Test
  public void shouldApplyTopicRenameTransforms() {
    Map<String, String> properties = new HashMap<>();
    properties.put(SourceConfigDefinition.TOPICS_REGEX.getKey(), "reroute.*");
    properties.put(TaskConfigDefinition.CONSUMER_CLIENT_ID, "testId");
    properties.put("name", "mockKafkaMonitor");
    properties.put("connector.class", "com.salesforce.mirus.MirusSourceConnector");
    properties.put("transforms", "reroute");
    properties.put("transforms.reroute.type", "org.apache.kafka.connect.transforms.RegexRouter");
    properties.put("transforms.reroute.regex", "^reroute\\.outgoing$");
    properties.put("transforms.reroute.replacement", "reroute.incoming");
    SourceConfig config = new SourceConfig(properties);

    MockConsumer<byte[], byte[]> mockSource = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
    updateMockPartitions(mockSource, "reroute.outgoing", 1);

    MockConsumer<byte[], byte[]> mockDest = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
    updateMockPartitions(mockDest, "reroute.incoming", 1);

    TaskConfigBuilder taskConfigBuilder =
        new TaskConfigBuilder(new RoundRobinTaskAssignor(), config);
    KafkaMonitor monitor =
        new KafkaMonitor(
            mock(ConnectorContext.class), config, mockSource, mockDest, taskConfigBuilder);

    monitor.partitionsChanged();
    List<Map<String, String>> result = monitor.taskConfigs(3);

    List<TopicPartition> partitions = assignedTopicPartitionsFromTaskConfigs(result);
    assertThat(partitions, contains(new TopicPartition("reroute.incoming", 0)));
  }
}
