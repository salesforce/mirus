package com.salesforce.mirus.metrics;

import java.lang.management.ManagementFactory;
import java.util.*;
import java.util.stream.Collectors;
import javax.management.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BufferMetrics {

  private static final Logger logger = LoggerFactory.getLogger(BufferMetrics.class);

  private static final String PRODUCER_METRIC_PREFIX =
      "kafka.producer:type=producer-metrics,client-id=";

  MBeanServer server = ManagementFactory.getPlatformMBeanServer();

  public List<String> getProducerNames() {
    Set<ObjectName> producerObjectNames = new HashSet<>();
    try {
      producerObjectNames = server.queryNames(new ObjectName(PRODUCER_METRIC_PREFIX + "*"), null);
    } catch (MalformedObjectNameException e) {
      logger.error(e.getMessage());
    }
    return producerObjectNames
        .stream()
        .map(objectName -> objectName.getCanonicalName())
        .collect(Collectors.toList());
  }

  public String getProducerName(String clientId) {
    Set<ObjectName> producerObjectNames = new HashSet<>();
    try {
      producerObjectNames = server.queryNames(new ObjectName(PRODUCER_METRIC_PREFIX + "*"), null);
    } catch (MalformedObjectNameException e) {
      logger.error(e.getMessage());
    }
    String canonName = producerObjectNames.iterator().next().getCanonicalName();
    String producerClientId =
        canonName.substring(canonName.indexOf("=") + 1, canonName.lastIndexOf("type=") - 1);
    String producerClientIdWithoutNumber =
        producerClientId.substring(0, producerClientId.lastIndexOf('-') + 1);

    Set<String> producerClientIds =
        producerObjectNames
            .stream()
            .map(objectName -> objectName.getCanonicalName())
            .map(name -> name.substring(name.indexOf("=") + 1, name.lastIndexOf("type=") - 1))
            .collect(Collectors.toSet());

    if (producerClientIds.contains(producerClientIdWithoutNumber + clientId)) {
      return producerClientIdWithoutNumber + clientId;
    }

      return Collections.max(producerClientIds);
  }

  public Double getBufferAvailableBytes(String producerName)
      throws InstanceNotFoundException, ReflectionException {
    Double bufferAvailableBytes = 0.0;
    ObjectName producerObjectName;
    try {
      producerObjectName = new ObjectName(PRODUCER_METRIC_PREFIX + producerName);
    } catch (MalformedObjectNameException e) {
      logger.error(e.getMessage());
      return bufferAvailableBytes;
    }
    try {
      bufferAvailableBytes =
          (Double) server.getAttribute(producerObjectName, "buffer-available-bytes");
    } catch (MBeanException | AttributeNotFoundException e) {
      logger.error(e.getMessage());
      return bufferAvailableBytes;
    }
    return bufferAvailableBytes;
  }

  public Double getBufferTotalBytes(String producerName)
      throws InstanceNotFoundException, ReflectionException {
    // Initialize to -1.0 because total bytes should never be less than or equal to 0
    Double bufferTotalBytes = -1.0;
    ObjectName producerObjectName;
    try {
      producerObjectName = new ObjectName(PRODUCER_METRIC_PREFIX + producerName);
    } catch (MalformedObjectNameException e) {
      logger.error(e.getMessage());
      return bufferTotalBytes;
    }

    try {
      bufferTotalBytes = (Double) server.getAttribute(producerObjectName, "buffer-total-bytes");
    } catch (MBeanException | AttributeNotFoundException e) {
      logger.error(e.getMessage());
      return bufferTotalBytes;
    }
    return bufferTotalBytes;
  }

  public boolean bufferAvailable() throws InstanceNotFoundException, ReflectionException {
    Set<ObjectName> producerMetrics = new HashSet<>();
    try {
      logger.trace("Querying producer metrics for each client-id.");
      producerMetrics =
          server.queryNames(
              new ObjectName("kafka.producer:type=producer-metrics,client-id=*"), null);
    } catch (MalformedObjectNameException e) {
      logger.error(e.getMessage());
    }
    logger.trace("Got {} producers", producerMetrics.size());
    for (ObjectName producer : producerMetrics) {
      Double bufferAvailableBytes = 0.0;
      try {
        // Buffer available bytes is of type Double
        bufferAvailableBytes = (Double) server.getAttribute(producer, "buffer-available-bytes");
      } catch (MBeanException | AttributeNotFoundException e) {
        logger.error(e.getMessage());
      }
      // If any producer's buffer available is 0, it counts as buffer unavailable.
      if (bufferAvailableBytes.equals(0.0)) {
        return false;
      }
    }
    return true;
  }
}
