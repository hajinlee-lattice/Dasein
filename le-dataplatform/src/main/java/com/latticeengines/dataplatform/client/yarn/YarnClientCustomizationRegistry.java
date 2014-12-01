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

    @Value("${dataplatform.fs.web.defaultFS}")
    protected String webHdfs;

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
        yarnConfiguration.set("dataplatform.hdfsJobBaseDir", hdfsJobBaseDir);
        yarnConfiguration.set("dataplatform.webHdfs", webHdfs);
        register(new DefaultYarnClientCustomization(yarnConfiguration));
        register(new PythonClientCustomization(yarnConfiguration));
        register(new RClientCustomization(yarnConfiguration));
        register(new EaiClientCustomization(yarnConfiguration));
    }

}
