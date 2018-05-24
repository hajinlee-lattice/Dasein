package com.latticeengines.apps.lp.entitymgr.impl;

import java.util.ArrayList;
import java.util.List;

import javax.inject.Inject;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.task.AsyncListenableTaskExecutor;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.stereotype.Component;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

import com.latticeengines.apps.lp.entitymgr.ModelSummaryDownloaderEntityMgr;
import com.latticeengines.apps.lp.entitymgr.ModelSummaryEntityMgr;
import com.latticeengines.apps.lp.service.BucketedScoreService;
import com.latticeengines.apps.lp.service.impl.FeatureImportanceParser;
import com.latticeengines.apps.lp.service.impl.ModelDownloaderCallable;
import com.latticeengines.db.exposed.entitymgr.TenantEntityMgr;
import com.latticeengines.domain.exposed.pls.ModelSummaryParser;
import com.latticeengines.domain.exposed.security.Tenant;

@Component("modelSummaryDownloaderEntityMgr")
public class ModelSummaryDownloaderEntityMgrImpl implements ModelSummaryDownloaderEntityMgr {

    private static final Logger log = LoggerFactory.getLogger(ModelSummaryDownloaderEntityMgrImpl.class);

    @Value("${pls.modelingservice.basedir}")
    private String modelingServiceHdfsBaseDir;

    @Value("${pls.downloader.max.pool.size}")
    private int concurrencyLimit;

    private AsyncListenableTaskExecutor modelSummaryDownloadListenableExecutor;

    @Inject
    private ModelSummaryEntityMgr modelSummaryEntityMgr;

    @Inject
    private TenantEntityMgr tenantEntityMgr;

    @Inject
    private Configuration yarnConfiguration;

    @Inject
    private ModelSummaryParser modelSummaryParser;

    @Inject
    private FeatureImportanceParser featureImportanceParser;

    @Inject
    private BucketedScoreService bucketedScoreService;

    public ListenableFuture<Boolean> downloadModel(Tenant tenant) {
        log.debug("Downloading model for tenant " + tenant.getId());
        ModelDownloaderCallable.Builder builder = new ModelDownloaderCallable.Builder();
        builder.modelServiceHdfsBaseDir(modelingServiceHdfsBaseDir) //
                .tenant(tenant) //
                .modelSummaryEntityMgr(modelSummaryEntityMgr) //
                .yarnConfiguration(yarnConfiguration) //
                .bucketedScoreService(bucketedScoreService) //
                .featureImportanceParser(featureImportanceParser) //
                .modelSummaryParser(modelSummaryParser);
        ModelDownloaderCallable callable = new ModelDownloaderCallable(builder);
        ListenableFuture<Boolean> task = modelSummaryDownloadListenableExecutor.submitListenable(callable);
        task.addCallback(new ListenableFutureCallback<Boolean>() {
            @Override
            public void onSuccess(Boolean result) {
                log.info("Tenant download complete!");
            }

            @Override
            public void onFailure(Throwable t) {
                log.error(t.toString());
            }
        });
        return task;
    }

    @Override
    public String downloadModel() {
        log.debug("ModelDownloader is ready to pick up models.");
        // timeStampContainer.setTimeStamp();
        // if (log.isDebugEnabled()) {
        // log.debug(String.valueOf(timeStampContainer.getTimeStamp().getSeconds()));
        // }
        SimpleAsyncTaskExecutor simpleTaskExecutor = new SimpleAsyncTaskExecutor();
        simpleTaskExecutor.setConcurrencyLimit(concurrencyLimit);

        modelSummaryDownloadListenableExecutor = simpleTaskExecutor;

        List<Tenant> tenants = tenantEntityMgr.findAll();
        List<ListenableFuture<Boolean>> futures = new ArrayList<>();
        for (Tenant tenant : tenants) {
            futures.add(downloadModel(tenant));
        }

        StringBuilder jobId = new StringBuilder();
        for (ListenableFuture<Boolean> future : futures) {
            jobId.append(Integer.toString(future.hashCode())).append("&&");
        }
        if (jobId.length() > 2) {
            jobId = new StringBuilder(jobId.substring(0, jobId.length() - 2));
        }
        return jobId.toString();
    }

}
