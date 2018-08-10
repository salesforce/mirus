/*
 *  Copyright (c) 2018, salesforce.com, inc.
 *  All rights reserved.
 *  SPDX-License-Identifier: BSD-3-Clause
 *  For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 *
 */

package com.salesforce.mirus.offsets;

import static java.util.stream.Collectors.groupingBy;

import com.salesforce.mirus.MirusSourceTask;
import com.salesforce.mirus.TopicPartitionSerDe;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.stream.Stream;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.storage.Converter;
import org.apache.kafka.connect.storage.KafkaOffsetBackingStore;
import org.apache.kafka.connect.storage.OffsetStorageWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Utility class for updating a set of offsets in a Kafka Connect offset backing store. */
class OffsetSetter {

  private static final Logger logger = LoggerFactory.getLogger(OffsetSetter.class);

  private final Converter internalConverter;
  private final KafkaOffsetBackingStore offsetBackingStore;

  OffsetSetter(Converter internalConverter, KafkaOffsetBackingStore offsetBackingStore) {
    this.internalConverter = internalConverter;
    this.offsetBackingStore = offsetBackingStore;
  }

  void setOffsets(Stream<OffsetInfo> offsetInfoStream) {
    // Set offsets for each connector
    offsetInfoStream
        .collect(groupingBy(v -> v.connectorId))
        .forEach(
            (connectorId, v) -> {
              logger.info("Writing offsets for " + connectorId);
              OffsetStorageWriter offsetWriter =
                  new OffsetStorageWriter(
                      offsetBackingStore, connectorId, internalConverter, internalConverter);
              v.forEach(
                  offsetInfo -> {
                    Map<String, Object> partition =
                        TopicPartitionSerDe.asMap(
                            new TopicPartition(offsetInfo.topic, offsetInfo.partition.intValue()));
                    Map<String, Long> offset = MirusSourceTask.offsetMap(offsetInfo.offset);
                    offsetWriter.offset(partition, offset);
                  });
              try {
                offsetBackingStore.start();
                offsetWriter.beginFlush();
                offsetWriter.doFlush(null).get();
                offsetBackingStore.stop();
              } catch (InterruptedException | ExecutionException e) {
                throw new RuntimeException("Unable to flush offsets for " + connectorId, e);
              }
            });
  }
}
