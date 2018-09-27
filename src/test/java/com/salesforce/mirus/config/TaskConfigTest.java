/*
 *  Copyright (c) 2018, salesforce.com, inc.
 *  All rights reserved.
 *  SPDX-License-Identifier: BSD-3-Clause
 *  For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 *
 */

package com.salesforce.mirus.config;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TaskConfigTest {

  private TaskConfig taskConfig;
  private Map<String, String> properties;

  @Before
  public void setUp() {
    properties = new HashMap<>();

    // Properties that should be dropped.
    properties.put("destination.bootstrap.servers", "dest:123");
    properties.put("tasks.max", "10");

    // Properties that should be passed through.
    properties.put("consumer.bootstrap.servers", "source:123");
    properties.put("poll.timeout.ms", "123");
    properties.put("destination.topic.name.prefix", "prefix");
    properties.put("destination.topic.name.suffix", "suffix");
    properties.put("enable.partition.matching", "true");
    properties.put("enable.buffer.flushing", "true");
    this.taskConfig = new TaskConfig(properties);
  }

  @Test
  public void missingValueShouldReturnDefault() {
    TaskConfig taskConfig = new TaskConfig(Collections.emptyMap());
    assertThat(taskConfig.getInternalTaskPartitions(), is(""));
    assertThat(taskConfig.getEnablePartitionMatching(), is(false));
    assertThat(taskConfig.getEnableBufferFlushing(), is(false));
  }

  @Test
  public void allExpectedPropertiesShouldBePassedThrough() {
    Map<String, Object> expectedProperties = new HashMap<>();
    expectedProperties.put("bootstrap.servers", "source:123");
    assertThat(this.taskConfig.getConsumerProperties(), is(expectedProperties));
    assertThat(this.taskConfig.getConsumerPollTimeout(), is(123L));
    assertThat(this.taskConfig.getDestinationTopicNamePrefix(), is("prefix"));
    assertThat(this.taskConfig.getDestinationTopicNameSuffix(), is("suffix"));
    assertThat(this.taskConfig.getEnablePartitionMatching(), is(true));
    assertThat(this.taskConfig.getEnableBufferFlushing(), is(true));
  }

  @Test
  public void filterShouldRemoveUnusedConfig() {
    Assert.assertThat(
        TaskConfig.filterProperties(properties).keySet(),
        containsInAnyOrder(
            "destination.topic.name.prefix",
            "destination.topic.name.suffix",
            "poll.timeout.ms",
            "consumer.bootstrap.servers",
            "enable.partition.matching",
            "enable.buffer.flushing"));
  }
}
