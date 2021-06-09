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

import org.junit.jupiter.api.Test;

public class OffsetInfoTest {

  @Test
  public void equalsShouldWork() {
    OffsetInfo offsetInfo1 = new OffsetInfo("a", "b", 1L, 2L);
    OffsetInfo offsetInfo2 = new OffsetInfo("a", "b", 1L, 2L);
    offsetInfo2.connectorId = "a";
    offsetInfo2.topic = "b";
    offsetInfo2.partition = 1L;
    offsetInfo2.offset = 2L;
    assertThat(offsetInfo1, is(offsetInfo2));
  }
}
