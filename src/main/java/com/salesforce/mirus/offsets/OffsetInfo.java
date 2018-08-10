/*
 *  Copyright (c) 2018, salesforce.com, inc.
 *  All rights reserved.
 *  SPDX-License-Identifier: BSD-3-Clause
 *  For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 *
 */

package com.salesforce.mirus.offsets;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import java.util.Objects;

@JsonPropertyOrder({"connectorId", "topic", "partition", "offset"})
class OffsetInfo {

  @JsonProperty String connectorId;
  @JsonProperty String topic;
  @JsonProperty Long partition;
  @JsonProperty Long offset;

  @JsonCreator
  OffsetInfo(
      @JsonProperty("connectorId") String connectorId,
      @JsonProperty("topic") String topic,
      @JsonProperty("partition") Long partition,
      @JsonProperty("offset") Long offset) {
    this.connectorId = connectorId;
    this.topic = topic;
    this.partition = partition;
    this.offset = offset;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    OffsetInfo that = (OffsetInfo) o;
    return Objects.equals(connectorId, that.connectorId)
        && Objects.equals(topic, that.topic)
        && Objects.equals(partition, that.partition)
        && Objects.equals(offset, that.offset);
  }

  @Override
  public int hashCode() {
    return Objects.hash(connectorId, topic, partition, offset);
  }
}
