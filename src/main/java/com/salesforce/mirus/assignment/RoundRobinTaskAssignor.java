/*
 *  Copyright (c) 2018, salesforce.com, inc.
 *  All rights reserved.
 *  SPDX-License-Identifier: BSD-3-Clause
 *  For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 *
 */

package com.salesforce.mirus.assignment;

import java.util.ArrayList;
import java.util.List;
import org.apache.kafka.common.TopicPartition;

/** Simple round-robin task assignor */
public class RoundRobinTaskAssignor implements SourceTaskAssignor {

  @Override
  public List<List<TopicPartition>> assign(List<TopicPartition> partitions, int numGroups) {
    List<List<TopicPartition>> assignment = new ArrayList<>();
    for (int i = 0; i < numGroups; i++) {
      assignment.add(new ArrayList<>());
    }

    int i = 0;
    for (TopicPartition partition : partitions) {
      assignment.get(i++ % numGroups).add(partition);
    }
    return assignment;
  }
}
