package com.latticeengines.hadoop.bean;

import java.util.Properties;

import javax.annotation.Resource;

import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.springframework.beans.factory.FactoryBean;

public class YarnConfigurationBeanFactory extends HadoopConfigurationBeanFactory<YarnConfiguration> implements FactoryBean<YarnConfiguration> {

    @Resource(name = "baseConfiguration")
    private YarnConfiguration baseConfiguration;

    @Override
    public Class<?> getObjectType() {
        return YarnConfiguration.class;
    }

    @Override
    public boolean isSingleton() {
        return true;
    }

    @Override
    protected YarnConfiguration getBaseConfiguration() {
        return baseConfiguration;
    }

    @Override
    protected YarnConfiguration getEmrConfiguration(String masterIp) {
        Properties properties = getYarnProperties(masterIp);
        YarnConfiguration yarnConfiguration = new YarnConfiguration();
        properties.forEach((k, v) -> yarnConfiguration.set((String) k, (String) v));
        return yarnConfiguration;
    }

}
