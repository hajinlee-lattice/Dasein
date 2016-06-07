package com.latticeengines.pls.service.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.springframework.core.task.AsyncTaskExecutor;

import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.pls.entitymanager.ModelSummaryEntityMgr;
import com.latticeengines.pls.mbean.TimeStampContainer;
import com.latticeengines.security.exposed.entitymanager.TenantEntityMgr;

public class ModelSummaryDownloadCallable implements Callable<Boolean> {

    private static final Log log = LogFactory.getLog(ModelSummaryDownloadCallable.class);

    private TenantEntityMgr tenantEntityMgr;
    private String modelServiceHdfsBaseDir;
    private ModelSummaryEntityMgr modelSummaryEntityMgr;
    private Configuration yarnConfiguration;
    private ModelSummaryParser parser;
    private FeatureImportanceParser featureImportanceParser;
    private AsyncTaskExecutor modelSummaryDownloadExecutor;
    private TimeStampContainer timeStampContainer;

    public ModelSummaryDownloadCallable(Builder builder) {
        this.tenantEntityMgr = builder.getTenantEntityMgr();
        this.modelServiceHdfsBaseDir = builder.getModelServiceHdfsBaseDir();
        this.modelSummaryEntityMgr = builder.getModelSummaryEntityMgr();
        this.yarnConfiguration = builder.getYarnConfiguration();
        this.parser = builder.getModelSummaryParser();
        this.featureImportanceParser = builder.getFeatureImportanceParser();
        this.modelSummaryDownloadExecutor = builder.getModelSummaryDownloadExecutor();
        this.timeStampContainer = builder.getTimeStampContainer();
    }

    private Future<Boolean> downloadModel(Tenant tenant) {
        log.info("Downloading model for tenant " + tenant.getId());
        ModelDownloaderCallable.Builder builder = new ModelDownloaderCallable.Builder();
        builder.modelServiceHdfsBaseDir(modelServiceHdfsBaseDir) //
                .tenant(tenant) //
                .modelSummaryEntityMgr(modelSummaryEntityMgr) //
                .yarnConfiguration(yarnConfiguration) //
                .modelSummaryParser(parser) //
                .featureImportanceParser(featureImportanceParser);
        ModelDownloaderCallable callable = new ModelDownloaderCallable(builder);
        return modelSummaryDownloadExecutor.submit(callable);
    }

    @Override
    public Boolean call() throws Exception {
        log.info("ModelDownloader is ready to pick up models.");
        timeStampContainer.setTimeStamp();

        if (log.isDebugEnabled()) {
            log.debug(timeStampContainer.getTimeStamp().getSeconds());
        }
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
                return false;
            }
        }
        return true;
    }

    public static class Builder {

        private TenantEntityMgr tenantEntityMgr;
        private String modelServiceHdfsBaseDir;
        private ModelSummaryEntityMgr modelSummaryEntityMgr;
        private Configuration yarnConfiguration;
        private ModelSummaryParser modelSummaryParser;
        private FeatureImportanceParser featureImportanceParser;
        private AsyncTaskExecutor modelSummaryDownloadExecutor;
        private TimeStampContainer timeStampContainer;

        public Builder() {

        }

        public Builder tenantEntityMgr(TenantEntityMgr tenantEntityMgr) {
            this.tenantEntityMgr = tenantEntityMgr;
            return this;
        }

        public Builder modelServiceHdfsBaseDir(String modelServiceHdfsBaseDir) {
            this.modelServiceHdfsBaseDir = modelServiceHdfsBaseDir;
            return this;
        }

        public Builder modelSummaryEntityMgr(ModelSummaryEntityMgr modelSummaryEntityMgr) {
            this.modelSummaryEntityMgr = modelSummaryEntityMgr;
            return this;
        }

        public Builder yarnConfiguration(Configuration yarnConfiguration) {
            this.yarnConfiguration = yarnConfiguration;
            return this;
        }

        public Builder modelSummaryParser(ModelSummaryParser modelSummaryParser) {
            this.modelSummaryParser = modelSummaryParser;
            return this;
        }

        public Builder featureImportanceParser(FeatureImportanceParser featureImportanceParser) {
            this.featureImportanceParser = featureImportanceParser;
            return this;
        }

        public Builder modelSummaryDownloadExecutor(AsyncTaskExecutor modelSummaryDownloadExecutor) {
            this.modelSummaryDownloadExecutor = modelSummaryDownloadExecutor;
            return this;
        }

        public Builder timeStampContainer(TimeStampContainer timeStampContainer) {
            this.timeStampContainer = timeStampContainer;
            return this;
        }

        public TenantEntityMgr getTenantEntityMgr() {
            return tenantEntityMgr;
        }

        public String getModelServiceHdfsBaseDir() {
            return modelServiceHdfsBaseDir;
        }

        public ModelSummaryEntityMgr getModelSummaryEntityMgr() {
            return modelSummaryEntityMgr;
        }

        public Configuration getYarnConfiguration() {
            return yarnConfiguration;
        }

        public ModelSummaryParser getModelSummaryParser() {
            return modelSummaryParser;
        }

        public FeatureImportanceParser getFeatureImportanceParser() {
            return featureImportanceParser;
        }

        public AsyncTaskExecutor getModelSummaryDownloadExecutor() {
            return modelSummaryDownloadExecutor;
        }

        public TimeStampContainer getTimeStampContainer() {
            return timeStampContainer;
        }

    }

}
