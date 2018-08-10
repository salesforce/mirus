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

import java.util.Collections;
import java.util.Map;
import org.junit.Test;

public class MirusConfigTest {

  @Test
  public void pollingCycleShouldBeQueryableWhenSet() {
    Map<String, String> properties =
        Collections.singletonMap("mirus.task.monitor.polling.interval.ms", "70000");
    MirusConfig mirusSourceConfig = new MirusConfig(properties);
    assertThat(mirusSourceConfig.getTaskStatePollingInterval(), is(70000L));
  }

  @Test
  public void taskAutoRestartShouldBeQueryableWhenSet() {
    Map<String, String> properties =
        Collections.singletonMap("mirus.task.auto.restart.enabled", "true");
    MirusConfig mirusSourceConfig = new MirusConfig(properties);
    assertThat(mirusSourceConfig.getTaskAutoRestart(), is(true));
  }

  @Test
  public void pollingCycleShouldGetDefaultWhenNotSet() {
    MirusConfig emptyMirusSourceConfig = new MirusConfig(Collections.emptyMap());
    assertThat(emptyMirusSourceConfig.getTaskStatePollingInterval(), is(120000L));
  }
}
