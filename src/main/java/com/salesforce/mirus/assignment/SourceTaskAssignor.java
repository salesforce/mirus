/*
 *  Copyright (c) 2018, salesforce.com, inc.
 *  All rights reserved.
 *  SPDX-License-Identifier: BSD-3-Clause
 *  For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 *
 */

package com.salesforce.mirus.assignment;

import java.util.List;
import org.apache.kafka.common.TopicPartition;

public interface SourceTaskAssignor {
  List<List<TopicPartition>> assign(List<TopicPartition> partitions, int numGroups);
}
