/*
 *  Copyright (c) 2018, salesforce.com, inc.
 *  All rights reserved.
 *  SPDX-License-Identifier: BSD-3-Clause
 *  For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 *
 */

package com.salesforce.mirus;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.junit.Before;
import org.junit.Test;

public class SourcePartitionValidatorTest {

  private MockConsumer<byte[], byte[]> mockConsumer;

  @Before
  public void setUp() {
    this.mockConsumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
    List<PartitionInfo> partitionInfoList =
        Arrays.asList(
            new PartitionInfo("topic1", 0, null, null, null),
            new PartitionInfo("topic1", 1, null, null, null));
    mockConsumer.updatePartitions("topic1", partitionInfoList);
  }

  @Test
  public void testIsHealthyWithTopicMatcher() {
    SourcePartitionValidator sourcePartitionHealthChecker =
        new SourcePartitionValidator(
            mockConsumer, SourcePartitionValidator.MatchingStrategy.TOPIC, Function.identity());
    assertThat(sourcePartitionHealthChecker.isHealthy(new TopicPartition("topic1", 0)), is(true));
    assertThat(sourcePartitionHealthChecker.isHealthy(new TopicPartition("topic1", 2)), is(true));
    assertThat(sourcePartitionHealthChecker.isHealthy(new TopicPartition("topic2", 0)), is(false));
  }

  @Test
  public void testIsHealthyWithPartitionMatcher() {
    SourcePartitionValidator sourcePartitionHealthChecker =
        new SourcePartitionValidator(
            mockConsumer, SourcePartitionValidator.MatchingStrategy.PARTITION, Function.identity());
    assertThat(sourcePartitionHealthChecker.isHealthy(new TopicPartition("topic1", 0)), is(true));
    assertThat(sourcePartitionHealthChecker.isHealthy(new TopicPartition("topic1", 2)), is(false));
    assertThat(sourcePartitionHealthChecker.isHealthy(new TopicPartition("topic2", 0)), is(false));
  }

  @Test
  public void shouldApplyTopicRenameWhenCheckingHealth() {
    MockConsumer<byte[], byte[]> consumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
    List<PartitionInfo> partitionInfoList =
        Arrays.asList(
            new PartitionInfo("replace1", 0, null, null, null),
            new PartitionInfo("replace1", 1, null, null, null));
    consumer.updatePartitions("replace1", partitionInfoList);

    SourcePartitionValidator sourcePartitionHealthChecker =
        new SourcePartitionValidator(
            consumer,
            SourcePartitionValidator.MatchingStrategy.TOPIC,
            t -> t.equals("topic1") ? "replace1" : t);
    assertThat(sourcePartitionHealthChecker.isHealthy(new TopicPartition("topic1", 0)), is(true));
    assertThat(sourcePartitionHealthChecker.isHealthy(new TopicPartition("topic1", 2)), is(true));
    assertThat(sourcePartitionHealthChecker.isHealthy(new TopicPartition("topic2", 0)), is(false));
  }
}
