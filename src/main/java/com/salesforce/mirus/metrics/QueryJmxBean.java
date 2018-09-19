package com.salesforce.mirus.metrics;

import java.lang.management.ManagementFactory;
import java.util.HashSet;
import java.util.Set;
import javax.management.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QueryJmxBean {

  private static final Logger logger = LoggerFactory.getLogger(QueryJmxBean.class);

  MBeanServer server = ManagementFactory.getPlatformMBeanServer();

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
      } catch (MBeanException e) {
        logger.error(e.getMessage());
      } catch (AttributeNotFoundException e) {
        logger.error(e.getMessage());
      }
      // If any producer's buffer is 0, it counts as buffer unavailable.
      if (bufferAvailableBytes.equals(0.0)) return false;
    }
    return true;
  }
}
