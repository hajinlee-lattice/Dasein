package com.latticeengines.release.jmx.service.impl;

import java.util.HashMap;

import javax.management.*;
import javax.management.remote.*;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import com.latticeengines.release.jmx.service.JMXCheckService;

@Service("jmxService")
public class JMXCheckServiceImpl implements JMXCheckService {

    @Value("${release.jmx.rmi}")
    private String jmxRMI;

    @Value("${release.jmx.object}")
    private String objectName;

    @Value("${release.jmx.operation}")
    private String operation;

    public String checkJMX() {
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
