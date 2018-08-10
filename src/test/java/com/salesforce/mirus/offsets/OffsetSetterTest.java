/*
 *  Copyright (c) 2018, salesforce.com, inc.
 *  All rights reserved.
 *  SPDX-License-Identifier: BSD-3-Clause
 *  For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 *
 */

package com.salesforce.mirus.offsets;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Future;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.storage.KafkaOffsetBackingStore;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class OffsetSetterTest {

  private OffsetSetter offsetSetter;

  @Mock private KafkaOffsetBackingStore kafkaOffsetBackingStore;

  @Mock private Future<Void> futureVoid;

  @Before
  public void setUp() {
    offsetSetter = new OffsetSetter(new JsonConverter(), kafkaOffsetBackingStore);
  }

  @Test
  public void shouldCallBackingStoreOncePerConnector() {
    List<OffsetInfo> offsetInfoList = new ArrayList<>();
    offsetInfoList.add(new OffsetInfo("connector-id1", "topic1", 1L, 123L));
    offsetInfoList.add(new OffsetInfo("connector-id1", "topic2", 1L, 123L));
    offsetInfoList.add(new OffsetInfo("connector-id2", "topic3", 1L, 123L));

    when(kafkaOffsetBackingStore.set(any(), any())).thenReturn(futureVoid);
    offsetSetter.setOffsets(offsetInfoList.stream());
    verify(kafkaOffsetBackingStore, times(2)).set(any(), any());
  }
}
