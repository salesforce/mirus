/*
 *  Copyright (c) 2018, salesforce.com, inc.
 *  All rights reserved.
 *  SPDX-License-Identifier: BSD-3-Clause
 *  For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 *
 */

package com.salesforce.mirus;

import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;

import com.salesforce.mirus.assignment.RoundRobinTaskAssignor;
import com.salesforce.mirus.config.SourceConfig;
import com.salesforce.mirus.config.SourceConfigDefinition;
import com.salesforce.mirus.config.TaskConfig;
import com.salesforce.mirus.config.TaskConfigDefinition;
import java.lang.Thread.State;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.connect.connector.ConnectorContext;
import org.junit.Before;
import org.junit.Test;

public class KafkaMonitorTest {

  private KafkaMonitor kafkaMonitor;
  private MockConsumer<byte[], byte[]> mockSourceConsumer;
  private MockConsumer<byte[], byte[]> mockDestinationConsumer;

  @Before
  public void setUp() {
    Map<String, String> properties = getBaseProperties();
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

  private Map<String, String> getBaseProperties() {
    Map<String, String> properties = new HashMap<>();
    properties.put(SourceConfigDefinition.TOPICS_REGEX.getKey(), "topic.*");
    properties.put(TaskConfigDefinition.CONSUMER_CLIENT_ID, "testId");
    properties.put("name", "mockKafkaMonitor");
    properties.put("connector.class", "com.salesforce.mirus.MirusSourceConnector");
    return properties;
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
    Map<String, String> properties = getBaseProperties();
    properties.put(SourceConfigDefinition.TOPICS_REGEX.getKey(), "reroute.*");
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
    assertThat(partitions, contains(new TopicPartition("reroute.outgoing", 0)));
  }

  @Test
  public void shouldAlwaysReplicateWhenCheckingDisabled() {
    Map<String, String> properties = getBaseProperties();
    properties.put(SourceConfigDefinition.ENABLE_DESTINATION_TOPIC_CHECKING.getKey(), "false");
    SourceConfig config = new SourceConfig(properties);
    TaskConfigBuilder taskConfigBuilder =
        new TaskConfigBuilder(new RoundRobinTaskAssignor(), config);
    KafkaMonitor monitor =
        new KafkaMonitor(
            mock(ConnectorContext.class),
            config,
            mockSourceConsumer,
            mockDestinationConsumer,
            taskConfigBuilder);
    monitor.partitionsChanged();
    List<Map<String, String>> result = monitor.taskConfigs(3);

    List<TopicPartition> partitions = assignedTopicPartitionsFromTaskConfigs(result);

    assertThat(partitions, hasItem(new TopicPartition("topic5", 0)));
  }

  @Test(expected = IllegalArgumentException.class)
  public void shouldThrowWhenUnsupportedTransformationEncountered() {
    Map<String, String> properties = getBaseProperties();
    properties.put("transforms", "reroute");
    properties.put(
        "transforms.reroute.type", "org.apache.kafka.connect.transforms.TimestampRouter");
    SourceConfig config = new SourceConfig(properties);
    TaskConfigBuilder taskConfigBuilder =
        new TaskConfigBuilder(new RoundRobinTaskAssignor(), config);

    new KafkaMonitor(
        mock(ConnectorContext.class),
        config,
        mockSourceConsumer,
        mockDestinationConsumer,
        taskConfigBuilder);
  }

  @Test
  public void shouldAllowUnsupportedTransformationWhenCheckingDisabled() {
    Map<String, String> properties = getBaseProperties();
    properties.put(SourceConfigDefinition.ENABLE_DESTINATION_TOPIC_CHECKING.getKey(), "false");
    properties.put("transforms", "reroute");
    properties.put(
        "transforms.reroute.type", "org.apache.kafka.connect.transforms.TimestampRouter");
    SourceConfig config = new SourceConfig(properties);
    TaskConfigBuilder taskConfigBuilder =
        new TaskConfigBuilder(new RoundRobinTaskAssignor(), config);
    KafkaMonitor monitor =
        new KafkaMonitor(
            mock(ConnectorContext.class),
            config,
            mockSourceConsumer,
            mockDestinationConsumer,
            taskConfigBuilder);
    monitor.partitionsChanged();
    List<Map<String, String>> result = monitor.taskConfigs(3);

    List<TopicPartition> partitions = assignedTopicPartitionsFromTaskConfigs(result);

    // All topics matching assignment regex, even ones not present in destination, should be
    // replicated
    assertThat(
        partitions,
        containsInAnyOrder(
            new TopicPartition("topic1", 0),
            new TopicPartition("topic1", 1),
            new TopicPartition("topic2", 0),
            new TopicPartition("topic3", 0),
            new TopicPartition("topic4", 0),
            new TopicPartition("topic5", 0)));
  }

  @Test
  public void shouldContinueRunningWhenExceptionEncountered() throws InterruptedException {
    Map<String, String> properties = getBaseProperties();
    SourceConfig config = new SourceConfig(properties);
    TaskConfigBuilder taskConfigBuilder =
        new TaskConfigBuilder(new RoundRobinTaskAssignor(), config);

    // Require two thrown exceptions to ensure that the KafkaMonitor run loop executes more than
    // once
    CountDownLatch exceptionThrownLatch = new CountDownLatch(2);
    MockConsumer<byte[], byte[]> consumer =
        new MockConsumer<byte[], byte[]>(OffsetResetStrategy.EARLIEST) {
          @Override
          public Map<String, List<PartitionInfo>> listTopics() {
            exceptionThrownLatch.countDown();
            throw new TimeoutException("KABOOM!");
          }
        };

    kafkaMonitor =
        new KafkaMonitor(
            mock(ConnectorContext.class),
            config,
            consumer,
            mockDestinationConsumer,
            taskConfigBuilder);
    Thread monitorThread = new Thread(kafkaMonitor);
    monitorThread.start();
    exceptionThrownLatch.await(2, TimeUnit.SECONDS);
    monitorThread.join(1);

    assertThat(monitorThread.getState(), not(State.TERMINATED));
    kafkaMonitor.stop();
    monitorThread.interrupt();
    monitorThread.join(5000);
  }
}
