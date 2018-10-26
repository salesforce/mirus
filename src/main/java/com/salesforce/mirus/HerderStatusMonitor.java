/*
 *  Copyright (c) 2018, salesforce.com, inc.
 *  All rights reserved.
 *  SPDX-License-Identifier: BSD-3-Clause
 *  For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 *
 */

package com.salesforce.mirus;

import com.salesforce.mirus.metrics.ConnectorJmxReporter;
import com.salesforce.mirus.metrics.TaskJmxReporter;
import java.util.Collection;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.runtime.ConnectorStatus;
import org.apache.kafka.connect.runtime.Herder;
import org.apache.kafka.connect.runtime.TaskStatus;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorStateInfo;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorStateInfo.TaskState;
import org.apache.kafka.connect.util.ConnectorTaskId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Thread that monitors Kafka Connect tasks and connectors. If enabled, it will attempt to
 * automatically restart failed tasks and connectors. JMX metrics will be also published to track
 * number of restart attempts and other useful metrics.
 */
public class HerderStatusMonitor implements Runnable {
  private static final Logger logger = LoggerFactory.getLogger(HerderStatusMonitor.class);

  private final Herder herder;
  private final String workerId;
  private final AtomicBoolean shutdown = new AtomicBoolean(false);
  private final CountDownLatch countDownLatch = new CountDownLatch(1);
  private final long pollingIntervalMillis;
  private final boolean autoRestartTaskEnabled;
  private final boolean autoRestartConnectorEnabled;
  private final ShutdownHook shutdownHook;
  private final TaskJmxReporter taskJmxReporter = new TaskJmxReporter();
  private final ConnectorJmxReporter connectorJmxReport = new ConnectorJmxReporter();

  HerderStatusMonitor(
      Herder herder,
      String workerId,
      long pollingIntervalMillis,
      boolean autoRestartTaskEnabled,
      boolean autoRestartConnectorEnabled) {
    this.herder = herder;
    this.workerId = workerId;
    this.pollingIntervalMillis = pollingIntervalMillis;
    this.autoRestartTaskEnabled = autoRestartTaskEnabled;
    this.autoRestartConnectorEnabled = autoRestartConnectorEnabled;
    this.shutdownHook = new ShutdownHook();
    logger.info("Task monitor thread will poll tasks every {} ms", pollingIntervalMillis);
  }

  @Override
  public void run() {
    logger.info("Starting a task monitor thread...");
    Runtime.getRuntime().addShutdownHook(shutdownHook);
    while (!shutdown.get()) {
      try {
        herder.connectors(this::onConnectors);
        try {
          boolean isCountZero = countDownLatch.await(pollingIntervalMillis, TimeUnit.MILLISECONDS);
          shutdown.set(isCountZero);
        } catch (InterruptedException e) {
          logger.info("Exiting TaskMonitor thread...");
        }
      } catch (ConnectException e) {
        logger.warn(
            "TaskMonitor thread will continue to execute despite exception. Caught exception: {}",
            e.getMessage());
      }
    }
  }

  private void onConnectors(Throwable error, Collection<String> connectorNames) {
    if (error != null) {
      logger.warn("Failed to retrieve connectors. Error details: {}", error);
      return;
    }

    connectorNames.forEach(this::processConnector);
  }

  private void processConnector(String connectorName) {
    ConnectorStateInfo stateInfo = herder.connectorStatus(connectorName);
    boolean isAssignedWorker = workerId.equals(stateInfo.connector().workerId());
    ConnectorStatus.State connectorState =
        ConnectorStatus.State.valueOf(stateInfo.connector().state());

    herder.connectorInfo(
        connectorName,
        (error, connectorInfo) -> {
          if (error != null) {
            logger.warn("Failed to retrieve connector info, Error details: {}", error);
            return;
          }
          // Only the worker assigned to this controller should report connector metrics
          // Report metrics regardless of current state
          if (isAssignedWorker) {
            connectorJmxReport.handleConnector(herder, connectorInfo);
          }

          if (connectorState == ConnectorStatus.State.RUNNING) {
            // All workers need to process all assigned tasks for the current connector
            connectorInfo.tasks().forEach(task -> processTask(task, herder.taskStatus(task)));
          }
        });

    // Only the assigned worker should attempt to restart a failed connector
    if (autoRestartConnectorEnabled
        && isAssignedWorker
        && connectorState == ConnectorStatus.State.FAILED) {
      logger.info("Attempting to restart connector {}", connectorName);
      connectorJmxReport.incrementConnectorRestartAttempts(connectorName);
      herder.restartConnector(
          connectorName,
          (error, _void) -> {
            if (error != null) {
              logger.warn("Failed to restart connector {}", connectorName, error);
            }
          });
    }
  }

  private void processTask(ConnectorTaskId taskId, TaskState taskStatus) {

    if (workerId.equals(taskStatus.workerId())) {
      taskJmxReporter.updateMetrics(taskId, taskStatus);
      TaskStatus.State taskState = TaskStatus.State.valueOf(taskStatus.state());
      if (taskState == TaskStatus.State.FAILED) {
        connectorJmxReport.incrementTotalFailedCount(taskId.connector());
        if (autoRestartTaskEnabled) {
          logger.info("Attempting to restart task {}", taskId);
          herder.restartTask(taskId, this::onTaskRestart);
        }
      }
    }
  }

  private void onTaskRestart(Throwable error, Void _void) {
    if (error != null) {
      logger.warn(
          "Failed to restart a task. This may have been caused by an active rebalance. Error details: {}",
          error);
    }
  }

  public void stop() {
    try {
      boolean wasShuttingDown = shutdown.getAndSet(true);
      if (!wasShuttingDown) {
        logger.info("TaskMonitor thread stopping");
      }
    } finally {
      countDownLatch.countDown();
      logger.info("TaskMonitor thread stopped");
    }
  }

  private class ShutdownHook extends Thread {

    @Override
    public void run() {
      HerderStatusMonitor.this.stop();
    }
  }
}
