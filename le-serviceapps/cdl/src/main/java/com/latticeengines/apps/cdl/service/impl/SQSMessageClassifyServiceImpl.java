package com.latticeengines.apps.cdl.service.impl;

import org.quartz.DisallowConcurrentExecution;
import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.quartz.QuartzJobBean;
import org.springframework.stereotype.Component;

import com.latticeengines.apps.cdl.service.S3ImportService;
import com.latticeengines.apps.cdl.service.SQSMessageClassifyService;

@DisallowConcurrentExecution
@Component("sqsMessageClassifyService")
public class SQSMessageClassifyServiceImpl extends QuartzJobBean implements SQSMessageClassifyService {

    private static final Logger log = LoggerFactory.getLogger(SQSMessageClassifyServiceImpl.class);

    @Override
    protected void executeInternal(JobExecutionContext jobExecutionContext) throws JobExecutionException {
        JobDataMap dataMap = jobExecutionContext.getJobDetail().getJobDataMap();
        boolean importEnabled = dataMap.getBoolean("importEnabled");
        if (importEnabled) {
            S3ImportService s3ImportService = (S3ImportService) dataMap.get("s3ImportService");
            if (s3ImportService == null) {
                log.error("S3ImportService is NULL, cannot update message URL");
            } else {
                log.info("Import enabled for current stack, start classify import message!");
                s3ImportService.updateMessageUrl();
            }
        } else {
            log.info("Import is not enabled on current stack. Skip update message url.");
        }
    }
}
