package com.salesforce.mirus.metrics;

import java.lang.management.ManagementFactory;
import java.util.Set;
import javax.management.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QueryJmxBean {

  private static final Logger logger = LoggerFactory.getLogger(QueryJmxBean.class);

  MBeanServer server = ManagementFactory.getPlatformMBeanServer();
  Set<ObjectInstance> instances = server.queryMBeans(null, null);
  Set<ObjectName> names = server.queryNames(null, null);

  public void getAllBeans()
      throws IntrospectionException, InstanceNotFoundException, ReflectionException {
    ObjectName producerMetrics = null;
    try {
      producerMetrics = new ObjectName("kafka.producer:type=producer-metrics");
    } catch (MalformedObjectNameException e) {
      e.printStackTrace();
    }

    MBeanInfo minfo = server.getMBeanInfo(producerMetrics);
    MBeanAttributeInfo ainfo[] = minfo.getAttributes();

    if (ainfo.length > 3) {
      logger.info("Wonderful");
    }

    for (ObjectName name : names) {
      MBeanInfo info = this.server.getMBeanInfo(name);
      logger.error("MBean Found:");
      logger.error("Class Name:t" + info.getClassName());
      logger.error("****************************************");
    }

    for (ObjectInstance instance : instances) {
      logger.error("Object Name:t" + instance.getObjectName());
      logger.error("****************************************");
    }
  }

  public void getBufferAvailableBytes() {
    return;
  }
}
