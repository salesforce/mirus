/*
 *  Copyright (c) 2018, salesforce.com, inc.
 *  All rights reserved.
 *  SPDX-License-Identifier: BSD-3-Clause
 *  For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 *
 */

package com.salesforce.mirus.config;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.contains;
import static org.junit.Assert.assertThat;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.transforms.Transformation;
import org.junit.Before;
import org.junit.Test;

public class SourceConfigTest {

  private SourceConfig mirusSourceConfig;

  public static class SimpleTransformation<R extends ConnectRecord<R>>
      implements Transformation<R> {

    int magicNumber = 0;

    @Override
    public int hashCode() {
      return this.magicNumber;
    }

    @Override
    public boolean equals(Object obj) {
      return (obj instanceof SimpleTransformation)
          && this.magicNumber == ((SimpleTransformation) obj).magicNumber;
    }

    @Override
    public void configure(Map<String, ?> props) {
      magicNumber = Integer.parseInt((String) props.get("magic.number"));
    }

    @Override
    public R apply(R record) {
      return null;
    }

    @Override
    public void close() {
      magicNumber = 0;
    }

    @Override
    public ConfigDef config() {
      return new ConfigDef()
          .define(
              "magic.number",
              ConfigDef.Type.INT,
              ConfigDef.NO_DEFAULT_VALUE,
              ConfigDef.Range.atLeast(42),
              ConfigDef.Importance.HIGH,
              "");
    }
  }

  @Before
  public void setUp() {
    Map<String, String> properties = new HashMap<>();
    properties.put("name", "testConnector");
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

  @Test
  public void transformationsShouldBeAvailable() {
    Map<String, String> properties = new HashMap<>();
    properties.put("name", "connector");
    properties.put("transforms", "a");
    properties.put("transforms.a.type", SimpleTransformation.class.getName());
    properties.put("transforms.a.magic.number", "45");
    SourceConfig configWithTransform = new SourceConfig(properties);

    List<Transformation<SourceRecord>> transformations = configWithTransform.transformations();

    SimpleTransformation<SourceRecord> expectedTransform = new SimpleTransformation<>();
    expectedTransform.configure(Collections.singletonMap("magic.number", "45"));
    assertThat(transformations, contains(expectedTransform));
  }
}
