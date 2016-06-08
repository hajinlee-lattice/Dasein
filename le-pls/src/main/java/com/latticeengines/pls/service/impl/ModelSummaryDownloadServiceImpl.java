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
import org.quartz.SchedulerContext;
import org.quartz.SchedulerException;
import org.springframework.context.ApplicationContext;
import org.springframework.core.task.AsyncTaskExecutor;
import org.springframework.scheduling.quartz.QuartzJobBean;
import org.springframework.stereotype.Component;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.TransactionCallbackWithoutResult;
import org.springframework.transaction.support.TransactionTemplate;

import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.pls.entitymanager.ModelSummaryEntityMgr;
import com.latticeengines.pls.mbean.TimeStampContainer;
import com.latticeengines.pls.service.ModelSummaryDownloadService;
import com.latticeengines.security.exposed.entitymanager.TenantEntityMgr;

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

    private TimeStampContainer timeStampContainer;
    
    private FeatureImportanceParser featureImportanceParser;

    public Future<Boolean> downloadModel(Tenant tenant) {
        log.debug("Downloading model for tenant " + tenant.getId());
        ModelDownloaderCallable.Builder builder = new ModelDownloaderCallable.Builder();
        builder.modelServiceHdfsBaseDir(modelingServiceHdfsBaseDir) //
            .tenant(tenant) //
            .modelSummaryEntityMgr(modelSummaryEntityMgr) //
            .yarnConfiguration(yarnConfiguration) //
            .modelSummaryParser(modelSummaryParser) //
            .featureImportanceParser(featureImportanceParser);
        ModelDownloaderCallable callable = new ModelDownloaderCallable(builder);
        return modelSummaryDownloadExecutor.submit(callable);
    }

    @Override
    protected void executeInternal(JobExecutionContext context) throws JobExecutionException {
        log.debug("ModelDownloader is ready to pick up models.");
        timeStampContainer.setTimeStamp();
        if (log.isDebugEnabled()) {
           log.debug(timeStampContainer.getTimeStamp().getSeconds());
        }

        final List<Future<Boolean>> futures = new ArrayList<>();

        SchedulerContext sc = null;
        try {
            sc = context.getScheduler().getContext();
        } catch (SchedulerException e) {
            log.error(e.getMessage(), e);
        }
        ApplicationContext appCtx = (ApplicationContext) sc.get("applicationContext");

        PlatformTransactionManager ptm = appCtx.getBean("transactionManager", PlatformTransactionManager.class);
        TransactionTemplate tx = new TransactionTemplate(ptm);
        tx.execute(new TransactionCallbackWithoutResult() {
            public void doInTransactionWithoutResult(TransactionStatus status) {
                List<Tenant> tenants = tenantEntityMgr.findAll();
                for (Tenant tenant : tenants) {
                    futures.add(downloadModel(tenant));
                }
            }
        });
        
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

    public TimeStampContainer getTimeStampContainer(){
        return this.timeStampContainer;
    }

    public void setTimeStampContainer(TimeStampContainer timeStampContainer){
        this.timeStampContainer = timeStampContainer;
    }
    
    public FeatureImportanceParser getFeatureImportanceParser() {
        return featureImportanceParser;
    }
    
    public void setFeatureImportanceParser(FeatureImportanceParser featureImportanceParser) {
        this.featureImportanceParser = featureImportanceParser;
    }
    
}
