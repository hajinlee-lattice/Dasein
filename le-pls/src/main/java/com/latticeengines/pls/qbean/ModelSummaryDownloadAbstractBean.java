package com.latticeengines.pls.qbean;

import java.util.concurrent.Callable;

import org.apache.hadoop.conf.Configuration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import com.latticeengines.pls.entitymanager.ModelSummaryDownloadFlagEntityMgr;
import com.latticeengines.pls.entitymanager.ModelSummaryEntityMgr;
import com.latticeengines.pls.mbean.TimeStampContainer;
import com.latticeengines.pls.service.BucketedScoreService;
import com.latticeengines.pls.service.impl.FeatureImportanceParser;
import com.latticeengines.pls.service.impl.ModelSummaryDownloadCallable;
import com.latticeengines.pls.service.impl.ModelSummaryParser;
import com.latticeengines.quartzclient.qbean.QuartzJobBean;
import com.latticeengines.security.exposed.entitymanager.TenantEntityMgr;

public abstract class ModelSummaryDownloadAbstractBean implements QuartzJobBean {

    @Value("${pls.modelingservice.basedir}")
    private String modelingServiceHdfsBaseDir;

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

    @Autowired
    private BucketedScoreService bucketedScoreService;

    @Value("${pls.downloader.max.pool.size}")
    private int maxPoolSize;

    @Value("${pls.downloader.core.pool.size}")
    private int corePoolSize;

    @Value("${pls.downloader.queue.capacity}")
    private int queueCapacity;

    @Autowired
    @Qualifier("taskExecutor")
    private ThreadPoolTaskExecutor taskExecutor;

    private boolean incremental;

    @Override
    public Callable<Boolean> getCallable(String jobArguments) {
        ModelSummaryDownloadCallable.Builder builder = new ModelSummaryDownloadCallable.Builder();
        builder.tenantEntityMgr(tenantEntityMgr)//
                .modelServiceHdfsBaseDir(modelingServiceHdfsBaseDir) //
                .modelSummaryEntityMgr(modelSummaryEntityMgr) //
                .bucketedScoreService(bucketedScoreService) //
                .yarnConfiguration(yarnConfiguration) //
                .modelSummaryParser(modelSummaryParser)
                .featureImportanceParser(featureImportanceParser)
                .modelSummaryDownloadExecutor(taskExecutor)
                .timeStampContainer(timeStampContainer)
                .modelSummaryDownloadFlagEntityMgr(modelSummaryDownloadFlagEntityMgr)
                .incremental(incremental);
        return new ModelSummaryDownloadCallable(builder);
    }

    protected void setIncremental(boolean incremental) {
        this.incremental = incremental;
    }
}
