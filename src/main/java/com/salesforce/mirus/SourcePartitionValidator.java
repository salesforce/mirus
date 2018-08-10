/*
 *  Copyright (c) 2018, salesforce.com, inc.
 *  All rights reserved.
 *  SPDX-License-Identifier: BSD-3-Clause
 *  For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 *
 */

package com.salesforce.mirus;

import java.util.Collection;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.TopicPartition;

/** Responsible for determining whether a source partition should be replicated. */
class SourcePartitionValidator {

  private static final int IGNORE_PARTITION_IDENTITY = -1;
  private final Consumer<byte[], byte[]> destinationConsumer;
  private final Set<TopicPartition> destinationPartitionIds;
  private final KeyStrategy keyStrategy;

  SourcePartitionValidator(
      Consumer<byte[], byte[]> destinationConsumer, MatchingStrategy matchingStrategy) {
    this.keyStrategy = matchingStrategy.keyStrategy;
    this.destinationConsumer = destinationConsumer;
    this.destinationPartitionIds = destinationPartitionIds();
  }

  private Set<TopicPartition> destinationPartitionIds() {
    synchronized (destinationConsumer) {
      return destinationConsumer
          .listTopics()
          .values()
          .stream()
          .flatMap(Collection::stream)
          .map(
              partitionInfo ->
                  keyStrategy.topicPartitionKey(partitionInfo.topic(), partitionInfo.partition()))
          .collect(Collectors.toSet());
    }
  }

  boolean isHealthy(TopicPartition topicPartition) {
    return destinationPartitionIds.contains(
        keyStrategy.topicPartitionKey(topicPartition.topic(), topicPartition.partition()));
  }

  enum MatchingStrategy {
    /** Match individual partitions. */
    PARTITION(TopicPartition::new),

    /** Match at the topic level. Partition identity is ignored. */
    TOPIC(
        (topic, partition) -> {
          // Ignore destination partition identity: as long as the topic exists in the destination
          // then the topic is included.
          return new TopicPartition(topic, IGNORE_PARTITION_IDENTITY);
        });

    private KeyStrategy keyStrategy;

    MatchingStrategy(KeyStrategy keyStrategy) {
      this.keyStrategy = keyStrategy;
    }
  }

  @FunctionalInterface
  private interface KeyStrategy {
    TopicPartition topicPartitionKey(String topic, int partition);
  }
}
