package com.latticeengines.apps.lp.qbean;

import java.util.concurrent.Callable;

import javax.annotation.Resource;
import javax.inject.Inject;

import org.apache.hadoop.conf.Configuration;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import com.latticeengines.apps.lp.entitymgr.ModelSummaryDownloadFlagEntityMgr;
import com.latticeengines.apps.lp.entitymgr.ModelSummaryEntityMgr;
import com.latticeengines.apps.lp.service.BucketedScoreService;
import com.latticeengines.apps.lp.service.ModelSummaryService;
import com.latticeengines.apps.lp.service.impl.FeatureImportanceParser;
import com.latticeengines.apps.lp.service.impl.ModelSummaryDownloadCallable;
import com.latticeengines.db.exposed.entitymgr.TenantEntityMgr;
import com.latticeengines.domain.exposed.pls.ModelSummaryParser;
import com.latticeengines.quartzclient.qbean.QuartzJobBean;

public abstract class ModelSummaryDownloadAbstractBean implements QuartzJobBean {

    @Value("${pls.modelingservice.basedir}")
    private String modelingServiceHdfsBaseDir;

    @Inject
    private ModelSummaryEntityMgr modelSummaryEntityMgr;

    @Inject
    private TenantEntityMgr tenantEntityMgr;

    @Inject
    private Configuration yarnConfiguration;

    @Inject
    private ModelSummaryParser modelSummaryParser;

    @Inject
    private TimeStampContainer timeStampContainer;

    @Inject
    private FeatureImportanceParser featureImportanceParser;

    @Inject
    private ModelSummaryDownloadFlagEntityMgr modelSummaryDownloadFlagEntityMgr;

    @Inject
    private BucketedScoreService bucketedScoreService;

    @Inject
    private ModelSummaryService modelSummaryService;

    @Value("${pls.downloader.max.pool.size}")
    private int maxPoolSize;

    @Value("${pls.downloader.core.pool.size}")
    private int corePoolSize;

    @Value("${pls.downloader.queue.capacity}")
    private int queueCapacity;

    @Resource(name = "commonTaskExecutor")
    private ThreadPoolTaskExecutor taskExecutor;

    private boolean incremental;

    @Override
    public Callable<Boolean> getCallable(String jobArguments) {
        ModelSummaryDownloadCallable.Builder builder = new ModelSummaryDownloadCallable.Builder() //
                .tenantEntityMgr(tenantEntityMgr) //
                .modelServiceHdfsBaseDir(modelingServiceHdfsBaseDir) //
                .modelSummaryEntityMgr(modelSummaryEntityMgr) //
                .bucketedScoreService(bucketedScoreService) //
                .yarnConfiguration(yarnConfiguration) //
                .modelSummaryParser(modelSummaryParser) //
                .featureImportanceParser(featureImportanceParser) //
                .modelSummaryDownloadExecutor(taskExecutor) //
                .timeStampContainer(timeStampContainer) //
                .modelSummaryDownloadFlagEntityMgr(modelSummaryDownloadFlagEntityMgr) //
                .incremental(incremental) //
                .modelSummaryService(modelSummaryService);
        return new ModelSummaryDownloadCallable(builder);
    }

    protected void setIncremental(boolean incremental) {
        this.incremental = incremental;
    }
}
