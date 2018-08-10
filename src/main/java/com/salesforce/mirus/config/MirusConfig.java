/*
 *  Copyright (c) 2018, salesforce.com, inc.
 *  All rights reserved.
 *  SPDX-License-Identifier: BSD-3-Clause
 *  For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 *
 */

package com.salesforce.mirus.config;

import java.util.Map;
import org.apache.kafka.connect.transforms.util.SimpleConfig;

public class MirusConfig {

  private final SimpleConfig simpleConfig;

  public MirusConfig(Map<String, String> properties) {
    this.simpleConfig = new SimpleConfig(MirusConfigDefinition.configDef(), properties);
  }

  public long getTaskStatePollingInterval() {
    return simpleConfig.getLong(MirusConfigDefinition.FAILED_TASK_POLLING_CYCLE.key);
  }

  public boolean getTaskAutoRestart() {
    return simpleConfig.getBoolean(MirusConfigDefinition.AUTO_RESTART_TASK_ENABLED.key);
  }

  public boolean getConnectorAutoRestart() {
    return simpleConfig.getBoolean(MirusConfigDefinition.AUTO_RESTART_CONNECTOR_ENABLED.key);
  }
}
