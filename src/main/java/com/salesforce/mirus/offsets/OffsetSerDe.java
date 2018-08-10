/*
 *  Copyright (c) 2018, salesforce.com, inc.
 *  All rights reserved.
 *  SPDX-License-Identifier: BSD-3-Clause
 *  For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 *
 */

package com.salesforce.mirus.offsets;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.dataformat.csv.CsvGenerator;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.util.stream.Stream;

/** A Serializer/Deserializer for offset data. */
class OffsetSerDe {

  private final ObjectWriter objectWriter;
  private final ObjectReader objectReader;

  OffsetSerDe(ObjectWriter objectWriter, ObjectReader objectReader) {
    this.objectWriter = objectWriter;
    this.objectReader = objectReader;
  }

  public void write(Stream<OffsetInfo> offsetInfoStream, OutputStream outputStream) {
    try (PrintWriter printWriter =
        new PrintWriter(new OutputStreamWriter(outputStream, StandardCharsets.UTF_8), true)) {
      offsetInfoStream.forEach(
          s -> {
            try {
              printWriter.println(objectWriter.writeValueAsString(s));
            } catch (JsonProcessingException e) {
              throw new RuntimeException(e);
            }
          });
    }
  }

  public Stream<OffsetInfo> read(Stream<String> stringStream) {
    return stringStream.map(
        s -> {
          try {
            return objectReader.readValue(s);
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        });
  }
}

class OffsetSerDeFactory {

  static OffsetSerDe create(Format format) {
    switch (format) {
      case CSV:
        return csvOffsetSerDe();
      default:
      case JSON:
        return jsonOffsetSerDe();
    }
  }

  private static OffsetSerDe csvOffsetSerDe() {
    CsvMapper csvMapper =
        new CsvMapper().configure(CsvGenerator.Feature.STRICT_CHECK_FOR_QUOTING, true);
    CsvSchema schema = csvMapper.schemaFor(OffsetInfo.class).withLineSeparator("");
    return new OffsetSerDe(
        csvMapper.writer(schema), csvMapper.reader(schema).forType(OffsetInfo.class));
  }

  private static OffsetSerDe jsonOffsetSerDe() {
    ObjectMapper objectMapper = new ObjectMapper();
    return new OffsetSerDe(objectMapper.writer(), objectMapper.reader().forType(OffsetInfo.class));
  }
}
