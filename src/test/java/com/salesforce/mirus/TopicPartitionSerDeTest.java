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
import org.apache.kafka.common.TopicPartition;
import org.junit.Before;
import org.junit.Test;

public class TopicPartitionSerDeTest {

  private List<TopicPartition> topicPartitionList;

  @Before
  public void setUp() {
    topicPartitionList =
        Arrays.asList(new TopicPartition("test", 0), new TopicPartition("test", 1));
  }

  @Test
  public void testToJson() {
    assertThat(
        TopicPartitionSerDe.toJson(topicPartitionList),
        is("[{\"topic\":\"test\",\"partition\":0},{\"topic\":\"test\",\"partition\":1}]"));
  }

  @Test
  public void testParseJson() {
    assertThat(
        TopicPartitionSerDe.parseTopicPartitionList(
            "[{\"topic\":\"test\",\"partition\":0},{\"topic\":\"test\",\"partition\":1}]"),
        is(topicPartitionList));
  }
}
