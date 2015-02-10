package com.latticeengines.pls.service.impl;

import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.quartz.QuartzJobBean;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.pls.entitymanager.TenantEntityMgr;
import com.latticeengines.pls.service.ModelSummaryDownloadService;

@Component("modelSummaryDownloadService")
public class ModelSummaryDownloadServiceImpl extends QuartzJobBean implements ModelSummaryDownloadService {
    
    private static final Log log = LogFactory.getLog(ModelSummaryDownloadServiceImpl.class);
    
    @Autowired
    private TenantEntityMgr tenantEntityMgr;

    @Override
    public void downloadModel(Tenant tenant) {
    }

    @Override
    protected void executeInternal(JobExecutionContext context) throws JobExecutionException {
        log.info("Downloading model for tenant.");
        
        List<Tenant> tenants = tenantEntityMgr.findAll();
        
        for (Tenant tenant : tenants) {
            downloadModel(tenant);
        }
    }

    public TenantEntityMgr getTenantEntityMgr() {
        return tenantEntityMgr;
    }

    public void setTenantEntityMgr(TenantEntityMgr tenantEntityMgr) {
        this.tenantEntityMgr = tenantEntityMgr;
    }
    
}
