package com.latticeengines.pls.qbean;

import java.util.concurrent.Callable;

import org.apache.hadoop.conf.Configuration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.stereotype.Component;

import com.latticeengines.pls.entitymanager.ModelSummaryDownloadFlagEntityMgr;
import com.latticeengines.pls.entitymanager.ModelSummaryEntityMgr;
import com.latticeengines.pls.mbean.TimeStampContainer;
import com.latticeengines.pls.service.impl.FeatureImportanceParser;
import com.latticeengines.pls.service.impl.ModelSummaryDownloadCallable;
import com.latticeengines.pls.service.impl.ModelSummaryParser;
import com.latticeengines.quartzclient.qbean.QuartzJobBean;
import com.latticeengines.security.exposed.entitymanager.TenantEntityMgr;

@Component("modelSummaryDownload")
public class ModelSummaryDownloadBean implements QuartzJobBean {

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

    @Autowired
    @Qualifier("taskExecutor")
    private ThreadPoolTaskExecutor taskExecutor;

    @Override
    public Callable<Boolean> getCallable(String jobArguments) {
        ModelSummaryDownloadCallable.Builder builder = new ModelSummaryDownloadCallable.Builder();
        builder.tenantEntityMgr(tenantEntityMgr)//
                .modelServiceHdfsBaseDir(modelingServiceHdfsBaseDir) //
                .modelSummaryEntityMgr(modelSummaryEntityMgr) //
                .yarnConfiguration(yarnConfiguration) //
                .modelSummaryParser(modelSummaryParser)
                .featureImportanceParser(featureImportanceParser)
                .modelSummaryDownloadExecutor(taskExecutor)
                .timeStampContainer(timeStampContainer)
                .modelSummaryDownloadFlagEntityMgr(modelSummaryDownloadFlagEntityMgr)
                .fullDownloadInterval(fullDownloadInterval)
                .maxPartialDownloadCount(maxPartialDownloadCount);
        return new ModelSummaryDownloadCallable(builder);
    }

}
