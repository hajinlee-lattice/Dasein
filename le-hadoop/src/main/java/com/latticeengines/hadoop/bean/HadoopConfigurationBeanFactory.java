package com.latticeengines.hadoop.bean;

import java.util.Properties;

import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.retry.support.RetryTemplate;

import com.latticeengines.common.exposed.util.RetryUtils;
import com.latticeengines.hadoop.service.EMRCacheService;

public abstract class HadoopConfigurationBeanFactory<T extends Configuration> implements FactoryBean<T> {

    private static final Logger log = LoggerFactory.getLogger(HadoopConfigurationBeanFactory.class);

    protected abstract T getBaseConfiguration();

    protected abstract T getEmrConfiguration(String masterIp);

    @Inject
    private EMRCacheService emrCacheService;

    @Value("${hadoop.use.emr}")
    private Boolean useEmr;

    @Value("${aws.emr.cluster}")
    private String clusterName;

    @Value("${aws.default.access.key}")
    protected String awsKey;

    @Value("${aws.default.secret.key.encrypted}")
    protected String awsSecret;

    @Override
    public T getObject() {
        T configuration;
        if (shouldUseEmr()) {
            RetryTemplate retryTemplate = RetryUtils.getExponentialBackoffRetryTemplate( //
                    5, 5000L, 2.0D, null);
            String masterIp = retryTemplate.execute(context -> emrCacheService.getMasterIp(clusterName));
            if (StringUtils.isBlank(masterIp)) {
                throw new RuntimeException("Cannot find the master IP for main EMR cluster.");
            }
            configuration = getEmrConfiguration(masterIp);
            configuration.set("hadoop.rpc.protection", "privacy");
        } else {
            configuration = getBaseConfiguration();
        }
        String fs = configuration.get("fs.defaultFS");
        log.info(String.format("Created a %s (%d): %s",
                getObjectType() == null ? "null" : getObjectType().getSimpleName(),
                System.identityHashCode(configuration), fs));
        return configuration;
    }

    @Override
    public boolean isSingleton() {
        return true;
    }

    protected Properties getYarnProperties(String masterIp) {
        return HadoopConfigurationUtils.loadPropsFromResource("emr.properties", masterIp, awsKey, awsSecret);
    }

    private boolean shouldUseEmr() {
        return Boolean.TRUE.equals(useEmr);
    }
}
