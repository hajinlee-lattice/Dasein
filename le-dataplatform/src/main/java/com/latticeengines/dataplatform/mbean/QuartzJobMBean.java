package com.latticeengines.dataplatform.mbean;

import org.springframework.beans.BeansException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.jmx.export.annotation.ManagedOperation;
import org.springframework.jmx.export.annotation.ManagedResource;
import org.springframework.stereotype.Component;

@Component("quartzJobMBean")
@ManagedResource(objectName = "Diagnostics:name=QuartzJobCheck")
public class QuartzJobMBean implements ApplicationContextAware {

    @Autowired
    private ApplicationContext applicationContext;

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        this.applicationContext = applicationContext;
    }

    @ManagedOperation(description = "Check if DLQuartzJob is Running")
    public String checkDLQuartzJob() {
        try {
            return applicationContext.getBean("dlorchestrationJob").toString();
        } catch (Exception e) {
            return "Failed to find DLQuartzJob in Spring ApplicationContext due to: \n" + e.getMessage();
        }
    }

    @ManagedOperation(description = "Check if QuartzJob is Running")
    public String checkQuartzJob() {
        try {
            return applicationContext.getBean("watchdogJob").toString();
        } catch (Exception e) {
            return "Failed to find QuartzJob in Spring ApplicationContext due to: \n" + e.getMessage();
        }
    }
}
