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
    properties.put("destination.consumer.b", "11,22,33");
    properties.put("destination.consumer.ssl.http.proxy.address", "");
    properties.put("source.consumer.poll.timeout.ms", "1000");
    properties.put("destination.consumer.topic.name.suffix", "suffix");
    properties.put("extra.key", "suffix");
    properties.put("consumer.a", "1");
    properties.put("consumer.b", "1,2,3");
    mirusSourceConfig = new SourceConfig(properties);
  }

  @Test
  public void destinationBootstrapPrecedenceIsRight(){
    Map<String, String> properties = new HashMap<>();
    properties.put("name", "testConnector");
    properties.put("destination.bootstrap.servers", "bad1:123,bad2:123");
    properties.put("destination.consumer.bootstrap.servers", "good1:123,good2:123");
    properties.put("consumer.a", "1");
    properties.put("destination.consumer.a", "11");
    properties.put("destination.consumer.b", "22");
    mirusSourceConfig = new SourceConfig(properties);

    Map<String, String> expectedDestConsumerProperties = new HashMap<>();
    expectedDestConsumerProperties.put("bootstrap.servers", "good1:123,good2:123");
    expectedDestConsumerProperties.put("a", "11");
    expectedDestConsumerProperties.put("b", "22");
    assertThat(
        mirusSourceConfig.getDestinationConsumerConfigs(), is(expectedDestConsumerProperties));
  }

  @Test
  public void destinationConsumerPropertiesShouldOverrideDefaultConsumerProps() {
    Map<String, String> expectedConsumerProperties = new HashMap<>();
    expectedConsumerProperties.put("a", "1");
    expectedConsumerProperties.put("b", "1,2,3");
    assertThat(mirusSourceConfig.getConsumerProperties(), is(expectedConsumerProperties));

    Map<String, String> expectedDestConsumerProperties = new HashMap<>();
    expectedDestConsumerProperties.put("bootstrap.servers", "remotehost1:123,remotehost2:123");
    expectedDestConsumerProperties.put("ssl.http.proxy.address", "");
    expectedDestConsumerProperties.put("b", "11,22,33");
    expectedDestConsumerProperties.put("a", "1");
    expectedDestConsumerProperties.put("topic.name.suffix", "suffix");
    assertThat(
        mirusSourceConfig.getDestinationConsumerConfigs(), is(expectedDestConsumerProperties));
  }

  @Test
  public void consumerPropertiesShouldBePassedThrough() {
    Map<String, String> expectedProperties = new HashMap<>();
    expectedProperties.put("a", "1");
    expectedProperties.put("b", "1,2,3");
    assertThat(mirusSourceConfig.getConsumerProperties(), is(expectedProperties));
  }

  @Test
  public void consumerDestinationPropertiesShouldIncludeBothDefaultAndDestinationProps() {
    Map<String, String> expectedProperties = new HashMap<>();
    expectedProperties.put("bootstrap.servers", "remotehost1:123,remotehost2:123");
    expectedProperties.put("ssl.http.proxy.address", "");
    expectedProperties.put("topic.name.suffix", "suffix");
    expectedProperties.put("a", "1");
    expectedProperties.put("b", "11,22,33");
    assertThat(mirusSourceConfig.getDestinationConsumerConfigs(), is(expectedProperties));
  }

  @Test
  public void defaultValuesShouldBeApplied() {
    assertThat(mirusSourceConfig.getTopicsRegex(), is(""));
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
