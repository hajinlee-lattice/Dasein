package com.latticeengines.pls.service.impl;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.springframework.scheduling.quartz.QuartzJobBean;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.pls.service.ModelSummaryDownloadService;

@Component("modelSummaryDownloadService")
public class ModelSummaryDownloadServiceImpl extends QuartzJobBean implements ModelSummaryDownloadService {
    
    private static final Log log = LogFactory.getLog(ModelSummaryDownloadServiceImpl.class);

    @Override
    public void downloadModel(Tenant tenant) {
        // TODO Auto-generated method stub
        
    }

    @Override
    protected void executeInternal(JobExecutionContext context) throws JobExecutionException {
        log.info("Downloading model for tenant.");
    }

}
