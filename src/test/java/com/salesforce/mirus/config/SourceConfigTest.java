/*
 *  Copyright (c) 2018, salesforce.com, inc.
 *  All rights reserved.
 *  SPDX-License-Identifier: BSD-3-Clause
 *  For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 *
 */

package com.salesforce.mirus.config;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.util.HashMap;
import java.util.Map;
import org.junit.Before;
import org.junit.Test;

public class SourceConfigTest {

  private SourceConfig mirusSourceConfig;

  @Before
  public void setUp() {
    Map<String, String> properties = new HashMap<>();
    properties.put("topics", "abc,def");
    properties.put("source.bootstrap.servers", "localhost:123");
    properties.put("destination.bootstrap.servers", "remotehost1:123,remotehost2:123");
    properties.put("source.consumer.poll.timeout.ms", "1000");
    properties.put("destination.topic.name.suffix", "suffix");
    properties.put("extra.key", "suffix");
    properties.put("consumer.a", "1");
    properties.put("consumer.b", "1,2,3");
    mirusSourceConfig = new SourceConfig(properties);
  }

  @Test
  public void consumerPropertiesShouldBePassedThrough() {
    Map<String, String> expectedProperties = new HashMap<>();
    expectedProperties.put("a", "1");
    expectedProperties.put("b", "1,2,3");
    assertThat(mirusSourceConfig.getConsumerProperties(), is(expectedProperties));
  }

  @Test
  public void defaultValuesShouldBeApplied() {
    assertThat(mirusSourceConfig.getTopicsRegex(), is(""));
  }

  @Test
  public void destinationBootstrapShouldBeAvailable() {
    assertThat(
        mirusSourceConfig.getDestinationBootstrapServers(), is("remotehost1:123,remotehost2:123"));
  }
}
