package com.latticeengines.cdl.workflow.listeners;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.JobExecution;
import org.springframework.stereotype.Component;

import com.latticeengines.workflow.listener.LEJobListener;

@Component("dataFeedTaskImportListener")
public class ImportListSegmentListener extends LEJobListener {

    private static final Logger log = LoggerFactory.getLogger(ImportListSegmentListener.class);

    @Override
    public void beforeJobExecution(JobExecution jobExecution) {
    }

    @Override
    public void afterJobExecution(JobExecution jobExecution) {

    }
}
