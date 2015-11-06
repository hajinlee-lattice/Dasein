package com.latticeengines.workflow.core;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.configuration.support.JobRegistryBeanPostProcessor;
import org.springframework.beans.BeansException;

public class CustomJobRegistryBeanPostProcessor extends JobRegistryBeanPostProcessor{

    private static final Log log = LogFactory.getLog(CustomJobRegistryBeanPostProcessor.class);
    @Override
    public Object postProcessAfterInitialization(Object bean, String beanName) throws BeansException {
        if (beanName.equals("singleContainerJob")) {
            log.info("Skipping " + beanName);
            return bean;
        } else {
            if (bean instanceof Job) {
                log.info("Registering Job " + beanName);
            }
            return super.postProcessAfterInitialization(bean, beanName);
        }
    }
}
