/*
 *  Copyright (c) 2018, salesforce.com, inc.
 *  All rights reserved.
 *  SPDX-License-Identifier: BSD-3-Clause
 *  For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 *
 */

package com.salesforce.mirus;

import java.util.Map;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.storage.Converter;

/**
 * This converter passes binary byte array data through unmodified.
 *
 * @deprecated Please use {@link org.apache.kafka.connect.converters.ByteArrayConverter}
 */
@Deprecated
public class ByteArrayConverter implements Converter {

  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {
    // Nothing to do.
  }

  @Override
  public byte[] fromConnectData(String topic, Schema schema, Object value) {
    return (byte[]) value;
  }

  @Override
  public SchemaAndValue toConnectData(String topic, byte[] value) {
    return new SchemaAndValue(Schema.BYTES_SCHEMA, value);
  }
}
