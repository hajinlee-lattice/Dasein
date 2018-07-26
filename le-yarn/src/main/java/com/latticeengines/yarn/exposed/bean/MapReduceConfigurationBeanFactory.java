package com.latticeengines.yarn.exposed.bean;

import java.util.Properties;

import javax.annotation.Resource;

import org.apache.hadoop.conf.Configuration;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.yarn.configuration.ConfigurationUtils;

import com.latticeengines.common.exposed.util.PropertyUtils;
import com.latticeengines.hadoop.bean.HadoopConfigurationBeanFactory;
import com.latticeengines.hadoop.bean.HadoopConfigurationUtils;

public class MapReduceConfigurationBeanFactory extends HadoopConfigurationBeanFactory implements FactoryBean<Configuration> {

    @Resource(name = "baseHadoopConfiguration")
    private Configuration baseHadoopConfiguration;

    @Override
    protected Configuration getBaseConfiguration() {
        return baseHadoopConfiguration;
    }

    @Override
    protected Configuration getEmrConfiguration(String masterIp) {
        Properties properties = getMRProperties(masterIp);
        return ConfigurationUtils.createFrom(new Configuration(), properties);
    }

    private Properties getMRProperties(String masterIp) {
        Properties properties = getYarnProperties(masterIp);
        Properties mrProps = HadoopConfigurationUtils.loadPropsFromResource("emr_mr.properties", masterIp);
        upsertProperty(mrProps, "mapreduce.map.memory.mb", "dataplatform.container.mapreduce.memory");
        upsertProperty(mrProps, "mapreduce.reduce.memory.mb", "dataplatform.container.mapreduce.memory");
        upsertProperty(mrProps, "mapreduce.map.cpu.vcores", "dataplatform.container.map.virtualcores");
        upsertProperty(mrProps, "mapreduce.reduce.cpu.vcores", "dataplatform.container.reduce.virtualcores");
        upsertProperty(mrProps, "mapreduce.task.timeout", "dataplatform.mapreduce.task.timeout");
        properties.putAll(mrProps);
        return properties;
    }

    private static void upsertProperty(Properties properties, String key, String valProp) {
        String value = PropertyUtils.getProperty(valProp);
        properties.setProperty(key, value);
    }
}
