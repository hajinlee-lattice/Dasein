package com.latticeengines.dataplatform.service.impl.watchdog;

import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

public class ConvertSuccessfulJobsToPMML extends WatchdogPlugin {

    public ConvertSuccessfulJobsToPMML() {
        register(this);
    }

    @Override
    public void run(JobExecutionContext context) throws JobExecutionException {
    }
}
