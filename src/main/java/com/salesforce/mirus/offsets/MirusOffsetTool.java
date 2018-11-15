/*
 *  Copyright (c) 2018, salesforce.com, inc.
 *  All rights reserved.
 *  SPDX-License-Identifier: BSD-3-Clause
 *  For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 *
 */

package com.salesforce.mirus.offsets;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.Map;
import java.util.stream.Stream;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.runtime.distributed.DistributedConfig;
import org.apache.kafka.connect.storage.Converter;
import org.apache.kafka.connect.storage.KafkaOffsetBackingStore;

/**
 * Tool for reading and writing Mirus offsets
 *
 * <p>Mirus stores offsets in a compacted Kafka topic.
 */
public class MirusOffsetTool {

  private final OffsetSerDe offsetSerDe;
  private final Args args;
  private final OffsetFetcher offsetFetcher;
  private final OffsetSetter offsetSetter;

  private MirusOffsetTool(
      Args args, OffsetFetcher offsetFetcher, OffsetSetter offsetSetter, OffsetSerDe offsetSerDe) {
    this.args = args;
    this.offsetFetcher = offsetFetcher;
    this.offsetSetter = offsetSetter;
    this.offsetSerDe = offsetSerDe;
  }

  public static void main(String[] argv) throws IOException {
    Args args = new Args();
    JCommander jCommander =
        JCommander.newBuilder()
            .programName(MirusOffsetTool.class.getSimpleName())
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

    if (args.resetOffsets && (args.fromFile == null || args.fromFile.isEmpty())) {
      throw new ParameterException("--reset-offsets requires --from-file to be set");
    }
    if (args.showNullOffsets && !args.describe) {
      throw new ParameterException("--show-nulls requires --describe to be set");
    }

    MirusOffsetTool mirusOffsetTool = newOffsetTool(args);
    mirusOffsetTool.run();
  }

  private static MirusOffsetTool newOffsetTool(Args args) throws IOException {
    // This needs to be the admin topic properties.
    // By default these are in the worker properties file, as this has the has admin producer and
    // consumer settings.  Separating these might be wise - also useful for storing state in
    // source cluster if it proves necessary.
    final Map<String, String> properties =
        !args.propertiesFile.isEmpty()
            ? Utils.propsToStringMap(Utils.loadProps(args.propertiesFile))
            : Collections.emptyMap();
    final DistributedConfig config = new DistributedConfig(properties);
    final KafkaOffsetBackingStore offsetBackingStore = new KafkaOffsetBackingStore();
    offsetBackingStore.configure(config);

    // Avoid initializing the entire Kafka Connect plugin system by assuming the
    // internal.[key|value].converter is org.apache.kafka.connect.json.JsonConverter
    final Converter internalConverter = new JsonConverter();
    internalConverter.configure(config.originalsWithPrefix("internal.key.converter."), true);

    final OffsetSetter offsetSetter = new OffsetSetter(internalConverter, offsetBackingStore);
    final OffsetFetcher offsetFetcher = new OffsetFetcher(config, internalConverter);
    final OffsetSerDe offsetSerDe = OffsetSerDeFactory.create(args.format);

    return new MirusOffsetTool(args, offsetFetcher, offsetSetter, offsetSerDe);
  }

  private void run() throws IOException {
    if (args.describe) {
      offsetFetcher.start();
      try {
        Stream<OffsetInfo> offsetInfos =
            offsetFetcher
                .readOffsets()
                .filter(offsetInfo -> offsetInfo.offset != null || args.showNullOffsets);
        offsetSerDe.write(offsetInfos, System.out);
      } finally {
        offsetFetcher.stop();
      }
    }
    if (args.resetOffsets) {
      offsetSetter.setOffsets(
          offsetSerDe.read(Files.readAllLines(Paths.get(args.fromFile)).stream()));
    }
  }

  static class Args {

    @Parameter(
        names = {"-f", "--properties-file"},
        description =
            "Kafka Connect Admin properties file.  By default this is the same as the Worker properties file.",
        required = true)
    String propertiesFile;

    @Parameter(
        names = {"--describe"},
        description = "Display all offsets stored in the offset storage topic")
    boolean describe;

    @Parameter(
        names = {"--show-nulls"},
        description = "Include records with null offsets (tombstone records). Requires --describe ")
    boolean showNullOffsets = false;

    @Parameter(
        names = {"--reset-offsets"},
        description =
            "Writes all offsets in --from-file to the offset storage topic. Requires: --from-file")
    boolean resetOffsets;

    @Parameter(
        names = {"--from-file"},
        description = "Path to a CSV formatted offset file.  Format as for --describe")
    String fromFile;

    @Parameter(
        names = {"--format"},
        description =
            "Format for reading and displaying offsets. In CSV mode the columns are <connector-id>,<topic>,<partition>,<offset>. Valid options: {CSV,JSON}")
    Format format = Format.CSV;

    @Parameter(names = "--help", help = true)
    boolean help = false;
  }
}
