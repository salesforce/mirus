/*
 *  Copyright (c) 2018, salesforce.com, inc.
 *  All rights reserved.
 *  SPDX-License-Identifier: BSD-3-Clause
 *  For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 *
 */

package com.salesforce.mirus;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.Test;

public class MirusTest {

  @Test
  public void overridesShouldInjectMissingKeys() throws Exception {
    List<String> overrides = Arrays.asList("test1=valueOverride1", "test2=valueOverride2");
    Map<String, String> properties = new HashMap<>();
    properties.put("test2", "value2");
    properties.put("test3", "value3");

    Mirus.applyOverrides(overrides, properties);
    assertThat(properties.keySet().size(), is(3));
    assertThat(properties.get("test1"), is("valueOverride1"));
    assertThat(properties.get("test2"), is("valueOverride2"));
    assertThat(properties.get("test3"), is("value3"));
  }

  @Test
  public void overridesWithMultipleEqualsShouldWork() throws Exception {
    List<String> overrides = Collections.singletonList("test1=value=with=equals");
    Map<String, String> properties = new HashMap<>();
    properties.put("test1", "value1");
    properties.put("test2", "value2");

    Mirus.applyOverrides(overrides, properties);

    assertThat(properties.keySet().size(), is(2));
    assertThat(properties.get("test1"), is("value=with=equals"));
    assertThat(properties.get("test2"), is("value2"));
  }
}
