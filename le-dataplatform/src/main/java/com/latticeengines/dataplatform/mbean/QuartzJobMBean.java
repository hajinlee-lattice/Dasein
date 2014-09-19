package com.latticeengines.dataplatform.mbean;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.jmx.export.annotation.ManagedOperation;
import org.springframework.jmx.export.annotation.ManagedResource;
import org.springframework.stereotype.Component;

@Component("quartzJobMBean")
@ManagedResource(objectName = "Diagnostics:name=QuartzJobCheck")
public class QuartzJobMBean {

    @Autowired
    private ApplicationContext applicationContext;

    @ManagedOperation(description = "Check if DLQuartzJob is Running")
    public String checkDLQuartzJob() {
        try {
            if (applicationContext.containsBean("dlorchestrationJob"))
                return "DLQuartzJob is enabled.";
            return "DLQuartzJob is disabled.";
        } catch (Exception e) {
            return "Failed to find DLQuartzJob in Spring ApplicationContext due to: \n" + e.getMessage();
        }
    }

    @ManagedOperation(description = "Check if QuartzJob is Running")
    public String checkQuartzJob() {
        try {
            if (applicationContext.containsBean("watchdogJob"))
                return "QuartzJob is enabled.";
            return "QuartzJob is disabled.";
        } catch (Exception e) {
            return "Failed to find QuartzJob in Spring ApplicationContext due to: \n" + e.getMessage();
        }
    }
}
