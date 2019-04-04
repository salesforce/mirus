/*
 *  Copyright (c) 2018, salesforce.com, inc.
 *  All rights reserved.
 *  SPDX-License-Identifier: BSD-3-Clause
 *  For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 *
 */

package com.salesforce.mirus;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.assertThat;

import com.salesforce.mirus.assignment.RoundRobinTaskAssignor;
import com.salesforce.mirus.config.SourceConfig;
import com.salesforce.mirus.config.TaskConfigDefinition;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.runtime.ConnectorConfig;
import org.junit.Before;
import org.junit.Test;

public class TaskConfigBuilderTest {

  private List<TopicPartition> topicPartitionList;

  private TaskConfigBuilder newBuilder(Map<String, String> properties) {
    properties.put(ConnectorConfig.NAME_CONFIG, "source-name");
    properties.put(TaskConfigDefinition.CONSUMER_CLIENT_ID, "test-");
    return new TaskConfigBuilder(new RoundRobinTaskAssignor(), new SourceConfig(properties));
  }

  @Before
  public void setUp() {
    topicPartitionList = new ArrayList<>();
    topicPartitionList.add(new TopicPartition("a", 1));
    topicPartitionList.add(new TopicPartition("a", 2));
    topicPartitionList.add(new TopicPartition("b", 1));
  }

  @Test
  public void shouldMakeClientIdsUniquePerTask() {
    Map<String, String> properties = new HashMap<>();
    List<Map<String, String>> result =
        newBuilder(properties).fromPartitionList(2, topicPartitionList);
    assertThat(result.size(), is(2));
    assertThat(
        result.get(0).get(TaskConfigDefinition.CONSUMER_CLIENT_ID), is("test-source-name-0"));
    assertThat(
        result.get(1).get(TaskConfigDefinition.CONSUMER_CLIENT_ID), is("test-source-name-1"));
  }

  @Test
  public void shouldIncludeConsumerProperties() {
    final String key = "consumer.include";
    final String value = "value";
    Map<String, String> properties = new HashMap<>();
    properties.put(key, value);
    List<Map<String, String>> result =
        newBuilder(properties).fromPartitionList(2, topicPartitionList);
    assertThat(result.size(), is(2));
    assertThat(result.get(0).get(key), is(value));
    assertThat(result.get(1).get(key), is(value));
  }

  @Test
  public void shouldExcludeUnknownProperties() {
    final String key = "other.exclude";
    Map<String, String> properties = new HashMap<>();
    properties.put(key, "1");
    List<Map<String, String>> result =
        newBuilder(properties).fromPartitionList(2, topicPartitionList);
    assertThat(result.size(), is(2));
    assertThat(result.get(0).get(key), nullValue());
    assertThat(result.get(1).get(key), nullValue());
  }

  @Test
  public void shouldRoundRobinPartitions() {
    Map<String, String> properties = new HashMap<>();
    List<Map<String, String>> result =
        newBuilder(properties).fromPartitionList(2, topicPartitionList);
    assertThat(result.size(), is(2));
    assertThat(
        result.get(0).get("partitions"),
        is(
            TopicPartitionSerDe.toJson(
                Arrays.asList(topicPartitionList.get(0), topicPartitionList.get(2)))));
    assertThat(
        result.get(1).get("partitions"),
        is(TopicPartitionSerDe.toJson(Collections.singletonList(topicPartitionList.get(1)))));
  }
}
