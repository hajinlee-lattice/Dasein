package com.latticeengines.pls.service.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.quartz.DisallowConcurrentExecution;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.springframework.core.task.AsyncTaskExecutor;
import org.springframework.scheduling.quartz.QuartzJobBean;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.pls.entitymanager.ModelSummaryEntityMgr;
import com.latticeengines.pls.entitymanager.TenantEntityMgr;
import com.latticeengines.pls.service.ModelSummaryDownloadService;

@DisallowConcurrentExecution
@Component("modelSummaryDownloadService")
public class ModelSummaryDownloadServiceImpl extends QuartzJobBean implements ModelSummaryDownloadService {
    
    private static final Log log = LogFactory.getLog(ModelSummaryDownloadServiceImpl.class);
    
    private String modelingServiceHdfsBaseDir;
    
    private AsyncTaskExecutor modelSummaryDownloadExecutor;
    
    private ModelSummaryEntityMgr modelSummaryEntityMgr;
    
    private TenantEntityMgr tenantEntityMgr;
    
    private Configuration yarnConfiguration;
    
    private ModelSummaryParser modelSummaryParser;

    public Future<Boolean> downloadModel(Tenant tenant) {
        log.info("Downloading model for tenant " + tenant.getId());
        ModelDownloaderCallable.Builder builder = new ModelDownloaderCallable.Builder();
        builder.modelServiceHdfsBaseDir(modelingServiceHdfsBaseDir) //
            .tenant(tenant) //
            .modelSummaryEntityMgr(modelSummaryEntityMgr) //
            .yarnConfiguration(yarnConfiguration) //
            .modelSummaryParser(modelSummaryParser);
        ModelDownloaderCallable callable = new ModelDownloaderCallable(builder);
        return modelSummaryDownloadExecutor.submit(callable);
    }

    @Override
    protected void executeInternal(JobExecutionContext context) throws JobExecutionException {
        List<Tenant> tenants = tenantEntityMgr.findAll();
        
        List<Future<Boolean>> futures = new ArrayList<>();
        for (Tenant tenant : tenants) {
            futures.add(downloadModel(tenant));
        }
        
        for (Future<Boolean> future : futures) {
            try {
                future.get();
            } catch (InterruptedException | ExecutionException e) {
                log.error(e);
            }
        }
    }

    public String getModelingServiceHdfsBaseDir() {
        return modelingServiceHdfsBaseDir;
    }

    public void setModelingServiceHdfsBaseDir(String modelingServiceHdfsBaseDir) {
        this.modelingServiceHdfsBaseDir = modelingServiceHdfsBaseDir;
    }

    public AsyncTaskExecutor getModelSummaryDownloadExecutor() {
        return modelSummaryDownloadExecutor;
    }

    public void setModelSummaryDownloadExecutor(AsyncTaskExecutor modelSummaryDownloadExecutor) {
        this.modelSummaryDownloadExecutor = modelSummaryDownloadExecutor;
    }

    public ModelSummaryEntityMgr getModelSummaryEntityMgr() {
        return modelSummaryEntityMgr;
    }

    public void setModelSummaryEntityMgr(ModelSummaryEntityMgr modelSummaryEntityMgr) {
        this.modelSummaryEntityMgr = modelSummaryEntityMgr;
    }
    
    public TenantEntityMgr getTenantEntityMgr() {
        return tenantEntityMgr;
    }

    public void setTenantEntityMgr(TenantEntityMgr tenantEntityMgr) {
        this.tenantEntityMgr = tenantEntityMgr;
    }

    public Configuration getYarnConfiguration() {
        return yarnConfiguration;
    }

    public void setYarnConfiguration(Configuration yarnConfiguration) {
        this.yarnConfiguration = yarnConfiguration;
    }

    public ModelSummaryParser getModelSummaryParser() {
        return modelSummaryParser;
    }

    public void setModelSummaryParser(ModelSummaryParser modelSummaryParser) {
        this.modelSummaryParser = modelSummaryParser;
    }

}
