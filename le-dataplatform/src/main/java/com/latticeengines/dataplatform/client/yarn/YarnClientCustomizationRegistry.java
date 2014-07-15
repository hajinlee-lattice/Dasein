package com.latticeengines.dataplatform.client.yarn;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component("yarnClientCustomizationRegistry")
public class YarnClientCustomizationRegistry implements InitializingBean {

    @Autowired
    private Configuration yarnConfiguration;
    
    @Value("${dataplatform.yarn.job.basedir}")
    private String hdfsJobBaseDir;

    private Map<String, YarnClientCustomization> registry = new HashMap<String, YarnClientCustomization>();

    public YarnClientCustomizationRegistry() {
    }

    public void register(YarnClientCustomization customization) {
        registry.put(customization.getClientId(), customization);
    }

    public YarnClientCustomization getCustomization(String clientId) {
        return registry.get(clientId);
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        register(new DefaultYarnClientCustomization(yarnConfiguration, hdfsJobBaseDir));
        register(new PythonClientCustomization(yarnConfiguration, hdfsJobBaseDir));
        register(new RClientCustomization(yarnConfiguration, hdfsJobBaseDir));
    }

}
