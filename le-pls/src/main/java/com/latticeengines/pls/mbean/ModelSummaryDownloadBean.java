package com.latticeengines.pls.mbean;

import java.util.concurrent.Callable;
import java.util.concurrent.ThreadPoolExecutor;

import javax.annotation.PostConstruct;

import com.latticeengines.pls.entitymanager.ModelSummaryDownloadFlagEntityMgr;
import org.apache.hadoop.conf.Configuration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.task.AsyncTaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.stereotype.Component;

import com.latticeengines.pls.entitymanager.ModelSummaryEntityMgr;
import com.latticeengines.pls.service.impl.FeatureImportanceParser;
import com.latticeengines.pls.service.impl.ModelSummaryDownloadCallable;
import com.latticeengines.pls.service.impl.ModelSummaryParser;
import com.latticeengines.quartzclient.mbean.QuartzJobBean;
import com.latticeengines.security.exposed.entitymanager.TenantEntityMgr;

@Component("modelSummaryDownload")
public class ModelSummaryDownloadBean implements QuartzJobBean {

    @Value("${pls.modelingservice.basedir}")
    private String modelingServiceHdfsBaseDir;

    private AsyncTaskExecutor modelSummaryDownloadExecutor;

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

    @Autowired
    private FeatureImportanceParser featureImportanceParser;

    @Autowired
    private ModelSummaryDownloadFlagEntityMgr modelSummaryDownloadFlagEntityMgr;

    @Value("${pls.downloader.max.pool.size}")
    private int maxPoolSize;

    @Value("${pls.downloader.core.pool.size}")
    private int corePoolSize;

    @Value("${pls.downloader.queue.capacity}")
    private int queueCapacity;

    @Value("${pls.downloader.full.download.interval:300}")
    private long fullDownloadInterval;

    @Value("${pls.downloader.partial.count:20}")
    private int maxPartialDownloadCount;

    @PostConstruct
    public void init() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setMaxPoolSize(maxPoolSize);
        executor.setCorePoolSize(corePoolSize);
        executor.setQueueCapacity(queueCapacity);
        executor.setRejectedExecutionHandler(new ThreadPoolExecutor.CallerRunsPolicy());
        executor.initialize();
        modelSummaryDownloadExecutor = executor;
    }

    @Override
    public Callable<Boolean> getCallable() {
        ModelSummaryDownloadCallable.Builder builder = new ModelSummaryDownloadCallable.Builder();
        builder.tenantEntityMgr(tenantEntityMgr)//
                .modelServiceHdfsBaseDir(modelingServiceHdfsBaseDir) //
                .modelSummaryEntityMgr(modelSummaryEntityMgr) //
                .yarnConfiguration(yarnConfiguration) //
                .modelSummaryParser(modelSummaryParser)
                .featureImportanceParser(featureImportanceParser)
                .modelSummaryDownloadExecutor(modelSummaryDownloadExecutor)
                .timeStampContainer(timeStampContainer)
                .modelSummaryDownloadFlagEntityMgr(modelSummaryDownloadFlagEntityMgr)
                .fullDownloadInterval(fullDownloadInterval)
                .maxPartialDownloadCount(maxPartialDownloadCount);
        return new ModelSummaryDownloadCallable(builder);
    }

}
