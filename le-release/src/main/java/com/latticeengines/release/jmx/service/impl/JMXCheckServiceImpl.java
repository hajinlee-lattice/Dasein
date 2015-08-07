package com.latticeengines.release.jmx.service.impl;

import java.util.HashMap;

import javax.management.*;
import javax.management.remote.*;
import org.springframework.stereotype.Service;

import com.latticeengines.release.jmx.service.JMXCheckService;

@Service("jmxService")
public class JMXCheckServiceImpl implements JMXCheckService {

    public String checkJMX(String jmxRMI, String objectName, String operation) {
        try {
            JMXServiceURL url = new JMXServiceURL(jmxRMI);
            JMXConnector jmxc = JMXConnectorFactory.connect(url, new HashMap<String, String>());
            MBeanServerConnection mbsc = jmxc.getMBeanServerConnection();
            Object result = mbsc.invoke(new ObjectName(objectName), operation, new Object[0], new String[0]);
            return String.valueOf(result);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

}
