package com.latticeengines.dataplatform.service;

import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

public interface JobWatchdogService {

    void run(JobExecutionContext context) throws JobExecutionException;

}
