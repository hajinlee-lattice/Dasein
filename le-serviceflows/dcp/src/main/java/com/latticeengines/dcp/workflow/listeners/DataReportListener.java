package com.latticeengines.dcp.workflow.listeners;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.JobExecution;
import org.springframework.stereotype.Component;

import com.latticeengines.workflow.listener.LEJobListener;

@Component("dataReportListener")
public class DataReportListener extends LEJobListener {

    public static final Logger log = LoggerFactory.getLogger(DataReportListener.class);

    @Override
    public void beforeJobExecution(JobExecution jobExecution) {
        log.info("Before job execution");

    }

    @Override
    public void afterJobExecution(JobExecution jobExecution) {
        log.info("Finish roll up data report");
    }
}
