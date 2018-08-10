/*
 *  Copyright (c) 2018, salesforce.com, inc.
 *  All rights reserved.
 *  SPDX-License-Identifier: BSD-3-Clause
 *  For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 *
 */

package com.salesforce.mirus;

import com.salesforce.mirus.assignment.SourceTaskAssignor;
import com.salesforce.mirus.config.SourceConfig;
import com.salesforce.mirus.config.TaskConfig;
import com.salesforce.mirus.config.TaskConfigDefinition;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigException;

/**
 * Responsible for building one Task Configuration map per Source Task. These are serialized and
 * sent to the Kafka Connect config topic.
 */
class TaskConfigBuilder {

  private final SourceTaskAssignor sourceTaskAssignor;
  private final Map<String, String> filteredConfig;
  private final String sourceName;

  TaskConfigBuilder(SourceTaskAssignor sourceTaskAssignor, SourceConfig config) {
    this.sourceTaskAssignor = sourceTaskAssignor;
    this.filteredConfig = TaskConfig.filterProperties(config.originalsStrings());
    this.sourceName = config.getName();
  }

  /** Generate a list of Task Configuration Maps from the current list of partitions. */
  public List<Map<String, String>> fromPartitionList(
      int maxTasks, List<TopicPartition> topicPartitionList) {

    // Assign partitions to tasks.
    List<List<TopicPartition>> partitionsByTask =
        sourceTaskAssignor.assign(
            topicPartitionList, Math.min(topicPartitionList.size(), maxTasks));

    // Generate configuration for each task.
    AtomicInteger taskCounter = new AtomicInteger();
    return partitionsByTask
        .stream()
        .map(TopicPartitionSerDe::toJson)
        .map(partitionList -> mapOf(TaskConfigDefinition.PARTITION_LIST, partitionList))
        .peek(t -> t.putAll(filteredConfig))
        .map(m -> makeClientIdUnique(m, taskCounter.getAndIncrement()))
        .collect(Collectors.toList());
  }

  private Map<String, String> makeClientIdUnique(Map<String, String> taskConfig, int taskCounter) {
    String clientId = taskConfig.get(TaskConfigDefinition.CONSUMER_CLIENT_ID);
    if (clientId == null) {
      throw new ConfigException("consumer.client.id must be set");
    }
    // Build a task id similar to the one used by Worker.
    String taskId = sourceName + "-" + taskCounter;
    taskConfig.put(TaskConfigDefinition.CONSUMER_CLIENT_ID, clientId + taskId);
    return taskConfig;
  }

  private Map<String, String> mapOf(String key, String value) {
    Map<String, String> map = new HashMap<>();
    map.put(key, value);
    return map;
  }
}
