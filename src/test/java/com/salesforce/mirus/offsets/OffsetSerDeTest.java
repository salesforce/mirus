/*
 *  Copyright (c) 2018, salesforce.com, inc.
 *  All rights reserved.
 *  SPDX-License-Identifier: BSD-3-Clause
 *  For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 *
 */

package com.salesforce.mirus.offsets;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

import java.io.ByteArrayOutputStream;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.Before;
import org.junit.Test;

public class OffsetSerDeTest {

  private OffsetSerDe offsetCsv;
  private OffsetSerDe offsetJson;
  private Stream<OffsetInfo> offsetInfoStream;
  private List<String> csvList;
  private List<String> jsonList;

  @Before
  public void setUp() {
    offsetCsv = OffsetSerDeFactory.create(Format.CSV);
    offsetJson = OffsetSerDeFactory.create(Format.JSON);

    List<OffsetInfo> offsetInfoList = new ArrayList<>();
    offsetInfoList.add(new OffsetInfo("connector-id", "topic", 1L, 123L));
    offsetInfoList.add(new OffsetInfo("connector-id", "topic", 2L, null));
    offsetInfoList.add(
        new OffsetInfo(
            "prd-logbus-source",
            "sfdc.test.logbus__prd.ajna_test__logs.coreapp.sp1.logbus",
            2L,
            345L));

    offsetInfoStream = offsetInfoList.stream();
    csvList =
        Arrays.asList(
            "connector-id,topic,1,123" + System.lineSeparator(),
            "connector-id,topic,2," + System.lineSeparator(),
            "prd-logbus-source,sfdc.test.logbus__prd.ajna_test__logs.coreapp.sp1.logbus,2,345"
                + System.lineSeparator());
    jsonList =
        Arrays.asList(
            "{\"connectorId\":\"connector-id\",\"topic\":\"topic\",\"partition\":1,\"offset\":123}"
                + System.lineSeparator(),
            "{\"connectorId\":\"connector-id\",\"topic\":\"topic\",\"partition\":2,\"offset\":null}"
                + System.lineSeparator(),
            "{\"connectorId\":\"prd-logbus-source\",\"topic\":\"sfdc.test.logbus__prd.ajna_test__logs.coreapp.sp1.logbus\",\"partition\":2,\"offset\":345}"
                + System.lineSeparator());
  }

  @Test
  public void offsetsWrittenCorrectlyAsCsv() throws UnsupportedEncodingException {
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    offsetCsv.write(offsetInfoStream, outputStream);
    assertThat(outputStream.toString(StandardCharsets.UTF_8.name()), is(String.join("", csvList)));
  }

  @Test
  public void offsetsReadCorrectlyAsCsv() {
    Stream<OffsetInfo> result = offsetCsv.read(csvList.stream());
    assertThat(
        result.collect(Collectors.toList()), is(offsetInfoStream.collect(Collectors.toList())));
  }

  @Test
  public void offsetsWrittenCorrectlyAsJson() throws UnsupportedEncodingException {
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    offsetJson.write(offsetInfoStream, outputStream);
    assertThat(outputStream.toString(StandardCharsets.UTF_8.name()), is(String.join("", jsonList)));
  }

  @Test
  public void offsetsReadCorrectlyAsJson() {
    Stream<OffsetInfo> result = offsetJson.read(jsonList.stream());
    assertThat(
        result.collect(Collectors.toList()), is(offsetInfoStream.collect(Collectors.toList())));
  }
}
