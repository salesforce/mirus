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
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.TopicPartition;

/** Responsible for determining whether a source partition should be replicated. */
class SourcePartitionValidator {

  private static final int IGNORE_PARTITION_IDENTITY = -1;
  private final Consumer<byte[], byte[]> destinationConsumer;
  private final Set<TopicPartition> destinationPartitionIds;
  private final KeyStrategy keyStrategy;
  private final Function<String, String> routerApplicationFunction;

  SourcePartitionValidator(
      Consumer<byte[], byte[]> destinationConsumer,
      MatchingStrategy matchingStrategy,
      Function<String, String> routerApplicationFunction) {
    this.keyStrategy = matchingStrategy.keyStrategy;
    this.destinationConsumer = destinationConsumer;
    this.routerApplicationFunction = routerApplicationFunction;
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
    TopicPartition targetTopicPartition = applyRoutersToTopic(topicPartition);
    return destinationPartitionIds.contains(
        keyStrategy.topicPartitionKey(
            targetTopicPartition.topic(), targetTopicPartition.partition()));
  }

  private TopicPartition applyRoutersToTopic(TopicPartition topicPartition) {
    String newTopic = this.routerApplicationFunction.apply(topicPartition.topic());
    return new TopicPartition(newTopic, topicPartition.partition());
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
