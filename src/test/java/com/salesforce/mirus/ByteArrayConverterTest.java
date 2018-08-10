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

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import org.junit.Before;
import org.junit.Test;

public class ByteArrayConverterTest {

  private static final byte[] TEST_BYTES = "TEST".getBytes(StandardCharsets.UTF_8);
  private ByteArrayConverter byteArrayConverter;

  @Before
  public void setUp() {
    byteArrayConverter = new ByteArrayConverter();
    byteArrayConverter.configure(new HashMap<>(), false);
  }

  @Test
  public void testFromConnectDataDoesNothing() {
    // The byte array should pass through unmodified.
    assertThat(byteArrayConverter.fromConnectData("ignored", null, TEST_BYTES), is(TEST_BYTES));
  }

  @Test
  public void testToConnectDataDoesNothing() {
    // The byte array should pass through unmodified.
    assertThat(byteArrayConverter.toConnectData("ignored", TEST_BYTES).value(), is(TEST_BYTES));
  }
}
