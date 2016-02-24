package com.latticeengines.pls.entitymanager.impl;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.task.AsyncListenableTaskExecutor;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.stereotype.Component;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.pls.entitymanager.ModelSummaryDownloaderEntityMgr;
import com.latticeengines.pls.entitymanager.ModelSummaryEntityMgr;
import com.latticeengines.pls.mbean.TimeStampContainer;
import com.latticeengines.pls.service.impl.ModelDownloaderCallable;
import com.latticeengines.pls.service.impl.ModelSummaryParser;
import com.latticeengines.security.exposed.entitymanager.TenantEntityMgr;

@Component("modelSummaryDownloaderEntityMgr")
public class ModelSummaryDownloaderEntityMgrImpl implements
        ModelSummaryDownloaderEntityMgr {

    private static final Log log = LogFactory
            .getLog(ModelSummaryDownloaderEntityMgrImpl.class);

    @Value("${pls.modelingservice.basedir}")
    private String modelingServiceHdfsBaseDir;

    @Value("${pls.downloader.max.pool.size}")
    private int concurrencyLimit;

    private AsyncListenableTaskExecutor modelSummaryDownloadListenableExecutor;

    @Autowired
    private ModelSummaryEntityMgr modelSummaryEntityMgr;

    @Autowired
    private TenantEntityMgr tenantEntityMgr;

    @Autowired
    private Configuration yarnConfiguration;

    @Autowired
    private ModelSummaryParser modelSummaryParser;

    @Autowired
    private TimeStampContainer timeStampContainer;

    public ListenableFuture<Boolean> downloadModel(Tenant tenant) {
        log.info("Downloading model for tenant " + tenant.getId());
        System.out.println("Downloading model for tenant " + tenant.getId());
        ModelDownloaderCallable.Builder builder = new ModelDownloaderCallable.Builder();
        builder.modelServiceHdfsBaseDir(modelingServiceHdfsBaseDir) //
                .tenant(tenant) //
                .modelSummaryEntityMgr(modelSummaryEntityMgr) //
                .yarnConfiguration(yarnConfiguration) //
                .modelSummaryParser(modelSummaryParser);
        ModelDownloaderCallable callable = new ModelDownloaderCallable(builder);
        ListenableFuture<Boolean> task = modelSummaryDownloadListenableExecutor
                .submitListenable(callable);
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
        log.info("ModelDownloader is ready to pick up models.");
        timeStampContainer.setTimeStamp();
        if (log.isDebugEnabled()) {
            log.debug(timeStampContainer.getTimeStamp().getSeconds());
        }
        SimpleAsyncTaskExecutor simpleTaskExecutor = new SimpleAsyncTaskExecutor();
        simpleTaskExecutor.setConcurrencyLimit(concurrencyLimit);

        modelSummaryDownloadListenableExecutor = simpleTaskExecutor;

        List<Tenant> tenants = tenantEntityMgr.findAll();
        List<ListenableFuture<Boolean>> futures = new ArrayList<>();
        for (Tenant tenant : tenants) {
            futures.add(downloadModel(tenant));
        }

        String jobId = "";
        for (ListenableFuture<Boolean> future : futures) {
            jobId += Integer.toString(future.hashCode()) + "&&";
        }
        if (jobId.length() > 2) {
            jobId = jobId.substring(0, jobId.length() - 2);
        }
        return jobId;
    }

}
