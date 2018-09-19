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
  Set<ObjectName> names;

  public Double[] getBufferAvailableBytes()
      throws IntrospectionException, InstanceNotFoundException, ReflectionException {

    try {
      names =
          server.queryNames(
              new ObjectName("kafka.producer:type=producer-metrics,client-id=*"), null);
    } catch (MalformedObjectNameException e) {
      e.printStackTrace();
    }
    Double bufferbytes[] = new Double[names.size()];
    int counter = 0;
    for (ObjectName name : names) {
      MBeanInfo minfo = server.getMBeanInfo(name);
      MBeanAttributeInfo ainfo[] = minfo.getAttributes();
      Object bufferinfo = null;
      try {
        bufferinfo = server.getAttribute(name, "buffer-available-bytes");
      } catch (MBeanException e) {
        e.printStackTrace();
      } catch (AttributeNotFoundException e) {
        e.printStackTrace();
      }
      bufferbytes[counter] = (Double) bufferinfo;
      counter++;
    }
    return bufferbytes;
  }
}
