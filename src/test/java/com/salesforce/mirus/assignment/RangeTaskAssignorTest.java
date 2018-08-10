/*
 *  Copyright (c) 2018, salesforce.com, inc.
 *  All rights reserved.
 *  SPDX-License-Identifier: BSD-3-Clause
 *  For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 *
 */

package com.salesforce.mirus.assignment;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.util.Arrays;
import java.util.List;
import org.apache.kafka.common.TopicPartition;
import org.junit.Before;
import org.junit.Test;

public class RangeTaskAssignorTest {

  private SourceTaskAssignor sourceTaskAssignor;

  @Before
  public void setUp() {
    sourceTaskAssignor = new RangeTaskAssignor();
  }

  @Test
  public void testAssignBalanced() {
    List<TopicPartition> partitions =
        Arrays.asList(
            new TopicPartition("a", 0),
            new TopicPartition("a", 1),
            new TopicPartition("a", 2),
            new TopicPartition("b", 0),
            new TopicPartition("b", 1),
            new TopicPartition("b", 2));
    List<List<TopicPartition>> result = sourceTaskAssignor.assign(partitions, 2);
    assertThat(
        result,
        is(
            Arrays.asList(
                Arrays.asList(
                    new TopicPartition("a", 0),
                    new TopicPartition("a", 1),
                    new TopicPartition("a", 2)),
                Arrays.asList(
                    new TopicPartition("b", 0),
                    new TopicPartition("b", 1),
                    new TopicPartition("b", 2)))));
  }

  @Test
  public void testAssignUnbalanced() {
    List<TopicPartition> partitions =
        Arrays.asList(
            new TopicPartition("a", 0),
            new TopicPartition("a", 1),
            new TopicPartition("a", 2),
            new TopicPartition("a", 3),
            new TopicPartition("b", 0),
            new TopicPartition("b", 1));

    List<List<TopicPartition>> result = sourceTaskAssignor.assign(partitions, 2);
    assertThat(
        result,
        is(
            Arrays.asList(
                Arrays.asList(
                    new TopicPartition("a", 0),
                    new TopicPartition("a", 1),
                    new TopicPartition("a", 2)),
                Arrays.asList(
                    new TopicPartition("a", 3),
                    new TopicPartition("b", 0),
                    new TopicPartition("b", 1)))));
  }
}
