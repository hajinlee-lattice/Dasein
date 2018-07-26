package com.latticeengines.hadoop.bean;

import java.util.Properties;

import javax.annotation.Resource;
import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.yarn.configuration.ConfigurationUtils;

import com.latticeengines.aws.emr.EMRService;

public class HadoopConfigurationBeanFactory implements FactoryBean<Configuration> {

    private static final String USE_EMR_YARN = "USE_EMR_YARN";

    @Resource(name = "baseConfiguration")
    private Configuration baseConfiguration;

    @Inject
    private EMRService emrService;

    @Override
    public Configuration getObject() {
        if (shouldUseEmr()) {
            String masterIp = emrService.getMasterIp();
            return getEmrConfiguration(masterIp);
        } else {
            return getBaseConfiguration();
        }
    }

    @Override
    public Class<?> getObjectType() {
        return Configuration.class;
    }

    @Override
    public boolean isSingleton() {
        return true;
    }

    protected Configuration getBaseConfiguration() {
        return baseConfiguration;
    }

    protected Configuration getEmrConfiguration(String masterIp) {
        Properties properties = getYarnProperties(masterIp);
        return ConfigurationUtils.createFrom(new Configuration(), properties);
    }

    protected Properties getYarnProperties(String masterIp) {
        return HadoopConfigurationUtils.loadPropsFromResource("emr.properties", masterIp);
    }

    private boolean shouldUseEmr() {
        // TODO: (ysong-M22) to be changed to use properties file
        return StringUtils.isNotBlank(System.getenv(USE_EMR_YARN))
                && Boolean.valueOf(System.getenv(USE_EMR_YARN));
    }
}
