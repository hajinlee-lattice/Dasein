package com.latticeengines.apps.lp.service.impl;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.task.AsyncTaskExecutor;

import com.latticeengines.apps.lp.entitymgr.ModelSummaryDownloadFlagEntityMgr;
import com.latticeengines.apps.lp.entitymgr.ModelSummaryEntityMgr;
import com.latticeengines.apps.lp.qbean.TimeStampContainer;
import com.latticeengines.apps.lp.service.BucketedScoreService;
import com.latticeengines.apps.lp.service.ModelSummaryService;
import com.latticeengines.db.exposed.entitymgr.TenantEntityMgr;
import com.latticeengines.domain.exposed.pls.ModelSummaryParser;
import com.latticeengines.domain.exposed.security.Tenant;

public class ModelSummaryDownloadCallable implements Callable<Boolean> {

    private static final Logger log = LoggerFactory.getLogger(ModelSummaryDownloadCallable.class);

    private TenantEntityMgr tenantEntityMgr;
    private String modelServiceHdfsBaseDir;
    private ModelSummaryEntityMgr modelSummaryEntityMgr;
    private BucketedScoreService bucketedScoreService;
    private Configuration yarnConfiguration;
    private ModelSummaryParser parser;
    private FeatureImportanceParser featureImportanceParser;
    private AsyncTaskExecutor modelSummaryDownloadExecutor;
    private TimeStampContainer timeStampContainer;
    private ModelSummaryDownloadFlagEntityMgr modelSummaryDownloadFlagEntityMgr;
    private boolean incremental;
    private ModelSummaryService modelSummaryService;

    public ModelSummaryDownloadCallable(Builder builder) {
        this.tenantEntityMgr = builder.getTenantEntityMgr();
        this.modelServiceHdfsBaseDir = builder.getModelServiceHdfsBaseDir();
        this.modelSummaryEntityMgr = builder.getModelSummaryEntityMgr();
        this.bucketedScoreService = builder.getBucketedScoreService();
        this.yarnConfiguration = builder.getYarnConfiguration();
        this.parser = builder.getModelSummaryParser();
        this.featureImportanceParser = builder.getFeatureImportanceParser();
        this.modelSummaryDownloadExecutor = builder.getModelSummaryDownloadExecutor();
        this.timeStampContainer = builder.getTimeStampContainer();
        this.modelSummaryDownloadFlagEntityMgr = builder.getModelSummaryDownloadFlagEntityMgr();
        this.incremental = builder.getIncremental();
        this.modelSummaryService = builder.getModelSummaryService();
    }

    private Future<Boolean> downloadModel(Tenant tenant, Set<String> modelSummaryIds) {
        log.debug("Downloading model for tenant " + tenant.getId());
        ModelDownloaderCallable.Builder builder = new ModelDownloaderCallable.Builder();
        builder.modelServiceHdfsBaseDir(modelServiceHdfsBaseDir) //
                .tenant(tenant) //
                .modelSummaryEntityMgr(modelSummaryEntityMgr) //
                .bucketedScoreService(bucketedScoreService) //
                .yarnConfiguration(yarnConfiguration) //
                .modelSummaryParser(parser) //
                .featureImportanceParser(featureImportanceParser)
                .modelSummaryIds(modelSummaryIds);
        ModelDownloaderCallable callable = new ModelDownloaderCallable(builder);
        return modelSummaryDownloadExecutor.submit(callable);
    }

    @Override
    public Boolean call() throws Exception {
        log.debug("ModelDownloader is ready to pick up models.");
        timeStampContainer.setTimeStamp();

        if (log.isDebugEnabled()) {
            log.debug(String.valueOf(timeStampContainer.getTimeStamp().getSeconds()));
        }

        if (!incremental) {
            Boolean result = fullDownload();
            return result;
        } else {
            Boolean result = partialDownload();
            if (result) {
                modelSummaryDownloadFlagEntityMgr.removeDownloadedFlag(System.currentTimeMillis()
                        - TimeUnit.HOURS.toMillis(24));
            }
            return result;
        }
    }

    private Boolean partialDownload() {
        long startTime = System.currentTimeMillis();
        List<String> waitingFlags = modelSummaryDownloadFlagEntityMgr.getWaitingFlags();
        List<String> excludeFlags = modelSummaryDownloadFlagEntityMgr.getExcludeFlags();
        HashSet<String> excludeTenantIds = null;
        if (excludeFlags != null && excludeFlags.size() > 0) {
            excludeTenantIds = new HashSet<>(excludeFlags);
        }
        long getWaitingFlagTime = System.currentTimeMillis() - startTime;
        log.debug(String.format("Get waiting flags duration: %d milliseconds.", getWaitingFlagTime));
        if (waitingFlags != null && waitingFlags.size() > 0) {
            HashSet<String> tenantIds = new HashSet<>(waitingFlags);
            Set<String> modelSummaryIds = modelSummaryService.getModelSummaryIds();
            List<Future<Boolean>> futures = new ArrayList<>();
            log.info(String.format("Begin download following tenants: %s", tenantIds.toString()));
            for (String tenantId : tenantIds) {
                if (excludeTenantIds == null || !excludeTenantIds.contains(tenantId)) {
                    Tenant tenant = tenantEntityMgr.findByTenantId(tenantId);
                    if (tenant != null) {
                        futures.add(downloadModel(tenant, modelSummaryIds));
                    }
                }
            }
            for (Future<Boolean> future : futures) {
                try {
                    future.get();
                } catch (InterruptedException | ExecutionException e) {
                    log.error(e.getMessage(), e);
                    return false;
                }
            }
            long totalSeconds = (System.currentTimeMillis() - startTime) / 1000;
            log.info(String.format("Partial download duration: %d seconds", totalSeconds));
        }
        return true;
    }

    private Boolean fullDownload() {
        long startTime = System.currentTimeMillis();
        List<String> waitingFlags = modelSummaryDownloadFlagEntityMgr.getWaitingFlags();
        HashSet<String> tenantIds = null;
        if (waitingFlags != null && waitingFlags.size() > 0) {
            tenantIds = new HashSet<>(waitingFlags);
        }
        List<Tenant> tenants = tenantEntityMgr.findAll();

        List<String> excludeFlags = modelSummaryDownloadFlagEntityMgr.getExcludeFlags();
        HashSet<String> excludeTenantIds = null;
        if (excludeFlags != null && excludeFlags.size() > 0) {
            excludeTenantIds = new HashSet<>(excludeFlags);
        }

        Set<String> modelSummaryIds = modelSummaryService.getModelSummaryIds();
        log.info(String.format("Full download for total %d tenants", tenants.size()));
        List<Future<Boolean>> futures = new ArrayList<>();
        for (Tenant tenant : tenants) {
            if ((tenantIds != null && tenantIds.contains(tenant.getId())) ||
                (excludeTenantIds != null && excludeTenantIds.contains(tenant.getId()))) {
                continue;
            }
            futures.add(downloadModel(tenant, modelSummaryIds));
        }

        for (Future<Boolean> future : futures) {
            try {
                future.get();
            } catch (InterruptedException | ExecutionException e) {
                log.error(e.getMessage(), e);
                return false;
            }
        }
        long totalSeconds = (System.currentTimeMillis() - startTime) / 1000;
        log.info(String.format("Full download duration: %d seconds", totalSeconds));
        return true;
    }

    public static class Builder {

        private TenantEntityMgr tenantEntityMgr;
        private String modelServiceHdfsBaseDir;
        private ModelSummaryEntityMgr modelSummaryEntityMgr;
        private BucketedScoreService bucketedScoreService;
        private Configuration yarnConfiguration;
        private ModelSummaryParser modelSummaryParser;
        private FeatureImportanceParser featureImportanceParser;
        private AsyncTaskExecutor modelSummaryDownloadExecutor;
        private TimeStampContainer timeStampContainer;
        private ModelSummaryDownloadFlagEntityMgr modelSummaryDownloadFlagEntityMgr;
        private boolean incremental;
        private ModelSummaryService modelSummaryService;

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

        public Builder bucketedScoreService(BucketedScoreService bucketedScoreService) {
            this.bucketedScoreService = bucketedScoreService;
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

        public Builder modelSummaryDownloadFlagEntityMgr(ModelSummaryDownloadFlagEntityMgr
                                                           modelSummaryDownloadFlagEntityMgr) {
            this.modelSummaryDownloadFlagEntityMgr = modelSummaryDownloadFlagEntityMgr;
            return this;
        }

        public Builder incremental(boolean incremental) {
            this.incremental = incremental;
            return this;
        }

        public Builder modelSummaryService(ModelSummaryService modelSummaryService) {
            this.modelSummaryService = modelSummaryService;
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

        public BucketedScoreService getBucketedScoreService() {
            return bucketedScoreService;
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

        public ModelSummaryDownloadFlagEntityMgr getModelSummaryDownloadFlagEntityMgr() {
            return modelSummaryDownloadFlagEntityMgr;
        }

        public boolean getIncremental() {
            return incremental;
        }

        public ModelSummaryService getModelSummaryService() { return modelSummaryService;}
    }

}
