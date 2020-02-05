package com.latticeengines.quartz.service;

import java.util.Date;

import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;
import org.springframework.scheduling.quartz.QuartzJobBean;

public class TestPrintMessageJob extends QuartzJobBean {

    private static final String MESSAGE = "message";

    @Override
    protected void executeInternal(JobExecutionContext context) {
        JobDataMap data = context.getJobDetail().getJobDataMap();
        String message = data.getString(MESSAGE);
        System.out.println(message + new Date());
    }

}
