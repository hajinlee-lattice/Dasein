package com.latticeengines.hadoop.bean;

import java.util.Properties;

import javax.annotation.Resource;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.yarn.configuration.ConfigurationUtils;

public class DistCpConfigurationBeanFactory extends HadoopConfigurationBeanFactory<YarnConfiguration> implements FactoryBean<YarnConfiguration> {

    @Resource(name = "yarnConfiguration")
    private YarnConfiguration baseConfiguration;

    @Value("${hadoop.use.ambari}")
    private boolean useAmbari;

    @Value("${hadoop.use.emr}")
    private Boolean useEmr;

    @Value("${hadoop.ambari.yarn.cp}")
    private String ambariYarnCp;

    @Value("${hadoop.ambari.mr.cp}")
    private String ambariMrCp;

    @Value("${hadoop.emr.yarn.cp}")
    private String emrYarnCp;

    @Value("${hadoop.emr.mr.cp}")
    private String emrMrCp;

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
        Properties properties = new Properties();
        if (!Boolean.TRUE.equals(useEmr) && useAmbari) {
            properties.setProperty("yarn.application.classpath", ambariYarnCp);
            properties.setProperty("mapreduce.application.classpath", ambariMrCp);

        } else if (Boolean.TRUE.equals(useEmr)) {
            properties.setProperty("yarn.application.classpath", emrYarnCp);
            properties.setProperty("mapreduce.application.classpath", emrMrCp);
        }
        return (YarnConfiguration) ConfigurationUtils.createFrom((Configuration) baseConfiguration, properties);
    }

    @Override
    protected YarnConfiguration getEmrConfiguration(String masterIp) {
        Properties properties = getYarnProperties(masterIp);
        YarnConfiguration yarnConfiguration = new YarnConfiguration();
        properties.forEach((k, v) -> yarnConfiguration.set((String) k, (String) v));
        return yarnConfiguration;
    }
}
