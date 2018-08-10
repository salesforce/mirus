/*
 *  Copyright (c) 2018, salesforce.com, inc.
 *  All rights reserved.
 *  SPDX-License-Identifier: BSD-3-Clause
 *  For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 *
 */

package com.salesforce.mirus;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.core.IsCollectionContaining.hasItem;
import static org.hamcrest.core.IsNot.not;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.salesforce.mirus.config.SourceConfigDefinition;
import com.salesforce.mirus.config.TaskConfigDefinition;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.kafka.common.config.ConfigDef;
import org.junit.Before;
import org.junit.Test;

public class MirusSourceConnectorTest {

  private MirusSourceConnector mirusSourceConnector;

  @Before
  public void setUp() {
    this.mirusSourceConnector = new MirusSourceConnector();
  }

  @Test
  public void testVersionStringIsValid() {
    String version = mirusSourceConnector.version();
    assertTrue(
        String.format("Version %s does not match", version),
        version.matches("^\\d+\\.\\d+\\.\\d+.*$"));
  }

  @Test
  public void testTaskClassIsCorrect() {
    assertThat(mirusSourceConnector.taskClass().toString(), is(MirusSourceTask.class.toString()));
  }

  @Test
  public void testStartAndStopBothComplete() {
    final KafkaMonitor mockKafkaMonitor = mock(KafkaMonitor.class);
    final List<Map<String, String>> testConfigs = Collections.emptyList();
    when(mockKafkaMonitor.taskConfigs(5)).thenReturn(testConfigs);

    mirusSourceConnector.start(mockKafkaMonitor);
    assertThat(mirusSourceConnector.taskConfigs(5), is(testConfigs));
    mirusSourceConnector.stop();
  }

  @Test
  public void testConfigReturnsValidDefinition() {
    ConfigDef configDef = mirusSourceConnector.config();
    assertThat(
        configDef.configKeys().keySet(), hasItem(SourceConfigDefinition.TOPICS_REGEX.getKey()));
    assertThat(configDef.configKeys().keySet(), not(hasItem(TaskConfigDefinition.PARTITION_LIST)));
  }
}
