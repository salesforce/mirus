/*
 *  Copyright (c) 2018, salesforce.com, inc.
 *  All rights reserved.
 *  SPDX-License-Identifier: BSD-3-Clause
 *  For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 *
 */

package com.salesforce.mirus;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.apache.kafka.common.TopicPartition;

/** Utility class for transforming TopicPartition instances in the Kafka Connect framework. */
public class TopicPartitionSerDe {
  static final String KEY_TOPIC = "topic";
  static final String KEY_PARTITION = "partition";

  private static final ObjectMapper OBJECT_MAPPER =
      new ObjectMapper().addMixIn(TopicPartition.class, TopicPartitionMixIn.class);

  public static Map<String, Object> asMap(TopicPartition topicPartition) {
    return ImmutableMap.of(
        KEY_TOPIC, topicPartition.topic(), KEY_PARTITION, topicPartition.partition());
  }

  static String toJson(List<TopicPartition> topicPartitionList) {
    try {
      return OBJECT_MAPPER.writeValueAsString(topicPartitionList);
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  static List<TopicPartition> parseTopicPartitionList(String partitionIdString) {
    try {
      return OBJECT_MAPPER.readValue(
          partitionIdString, new TypeReference<List<TopicPartition>>() {});
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}

abstract class TopicPartitionMixIn {
  TopicPartitionMixIn(
      @JsonProperty("topic") String topic, @JsonProperty("partition") int partition) {}

  @JsonProperty("topic")
  abstract String topic();

  @JsonProperty("partition")
  abstract int partition();
}
