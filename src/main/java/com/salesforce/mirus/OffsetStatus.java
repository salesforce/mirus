/*
 *  Copyright (c) 2018, salesforce.com, inc.
 *  All rights reserved.
 *  SPDX-License-Identifier: BSD-3-Clause
 *  For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 *
 */

package com.salesforce.mirus;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

/** Display current start and end offsets for a standard Kafka topic. */
public class OffsetStatus {

  private final Args args;

  private OffsetStatus(Args args) {
    this.args = args;
  }

  public static void main(String[] argv) {
    Args args = new Args();
    JCommander jCommander =
        JCommander.newBuilder()
            .programName(OffsetStatus.class.getSimpleName())
            .addObject(args)
            .build();
    try {
      jCommander.parse(argv);
    } catch (Exception e) {
      jCommander.usage();
      throw e;
    }
    if (args.help) {
      jCommander.usage();
      return;
    }
    new OffsetStatus(args).run();
  }

  private void run() {
    Map<String, Object> consumerProperties = new HashMap<>();
    consumerProperties.put("bootstrap.servers", args.bootstrapServers);
    consumerProperties.put(
        "key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
    consumerProperties.put(
        "value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");

    if (args.ssl) {
      consumerProperties.put("security.protocol", "SSL");
    }

    try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(consumerProperties)) {
      List<PartitionInfo> pi = consumer.partitionsFor(args.topic);
      List<TopicPartition> tp =
          pi.stream()
              .map(
                  partitionInfo ->
                      new TopicPartition(partitionInfo.topic(), partitionInfo.partition()))
              .collect(Collectors.toList());
      Map<TopicPartition, Long> beginningOffsets = consumer.beginningOffsets(tp);
      Map<TopicPartition, Long> endOffsets = consumer.endOffsets(tp);
      System.out.println("Topic\tPartition\tStart Offset\tEnd Offset");
      beginningOffsets
          .keySet()
          .forEach(
              k ->
                  System.out.printf(
                      "%s\t%d\t%d\t%d%n",
                      k.topic(), k.partition(), beginningOffsets.get(k), endOffsets.get(k)));
    }
  }

  static class Args {

    @Parameter(
        names = {"-t", "--topic"},
        description = "Kafka topic name",
        required = true)
    String topic;

    @Parameter(
        names = {"-b", "--bootstrap-server"},
        required = true)
    String bootstrapServers;

    @Parameter(names = "--ssl", description = "Use security.protocol=ssl")
    boolean ssl;

    @Parameter(names = "--help", help = true)
    boolean help = false;
  }
}
