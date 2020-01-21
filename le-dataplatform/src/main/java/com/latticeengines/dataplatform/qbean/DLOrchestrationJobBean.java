package com.latticeengines.dataplatform.qbean;

import java.util.concurrent.Callable;

import javax.annotation.Resource;
import javax.inject.Inject;

import org.apache.hadoop.conf.Configuration;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Lazy;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.stereotype.Component;

import com.latticeengines.dataplatform.entitymanager.ModelCommandEntityMgr;
import com.latticeengines.dataplatform.entitymanager.ModelCommandResultEntityMgr;
import com.latticeengines.dataplatform.entitymanager.ModelCommandStateEntityMgr;
import com.latticeengines.dataplatform.entitymanager.ModelDownloadFlagEntityMgr;
import com.latticeengines.dataplatform.service.dlorchestration.ModelCommandLogService;
import com.latticeengines.dataplatform.service.dlorchestration.ModelStepProcessor;
import com.latticeengines.dataplatform.service.dlorchestration.ModelStepYarnProcessor;
import com.latticeengines.dataplatform.service.impl.dlorchestration.DLOrchestrationCallable;
import com.latticeengines.dataplatform.service.impl.dlorchestration.DebugProcessorImpl;
import com.latticeengines.dataplatform.service.modeling.ModelingJobService;
import com.latticeengines.db.exposed.service.DbMetadataService;
import com.latticeengines.monitor.exposed.alerts.service.AlertService;
import com.latticeengines.quartzclient.qbean.QuartzJobBean;

@Lazy
@Component("dlOrchestrationQuartzJob")
public class DLOrchestrationJobBean implements QuartzJobBean {

    @Resource(name = "commonTaskExecutor")
    private ThreadPoolTaskExecutor taskExecutor;

    @Inject
    private ModelCommandEntityMgr modelCommandEntityMgr;

    @Inject
    private ModelingJobService modelingJobService;

    @Inject
    private ModelCommandStateEntityMgr modelCommandStateEntityMgr;

    @Inject
    private ModelStepYarnProcessor modelStepYarnProcessor;

    @Inject
    private ModelCommandLogService modelCommandLogService;

    @Inject
    private ModelCommandResultEntityMgr modelCommandResultEntityMgr;

    @Inject
    private ModelDownloadFlagEntityMgr modelDownloadFlagEntityMgr;

    @Inject
    private ModelStepProcessor modelStepFinishProcessor;

    @Inject
    private ModelStepProcessor modelStepOutputResultsProcessor;

    @Inject
    private ModelStepProcessor modelStepRetrieveMetadataProcessor;

    @Inject
    private AlertService alertService;

    @Inject
    private DebugProcessorImpl debugProcessorImpl;

    @Value("${dataplatform.dlorchestrationjob.wait.time}")
    private int waitTime = 180;

    @Inject
    private Configuration yarnConfiguration;

    @Value("${hadoop.yarn.resourcemanager.webapp.address}")
    private String resourceManagerWebAppAddress;

    @Value("${hadoop.yarn.timeline-service.webapp.address}")
    private String appTimeLineWebAppAddress;

    @Value("${dataplatform.dlorchestrationjob.row.fail.threshold}")
    private int rowFailThreshold;

    @Value("${dataplatform.dlorchestrationjob.row.warn.threshold}")
    private int rowWarnThreshold;

    @Value("${dataplatform.dlorchestrationjob.postiveevent.fail.threshold}")
    private int positiveEventFailThreshold;

    @Value("${dataplatform.dlorchestrationjob.postiveevent.warn.threshold}")
    private int positiveEventWarnThreshold;

    private int featuresThreshold = -1;

    @Inject
    private DbMetadataService dbMetadataService;

    @Value("${dataplatform.dlorchestrationjob.max.pool.size}")
    private int maxPoolSize;

    @Value("${dataplatform.dlorchestrationjob.core.pool.size}")
    private int corePoolSize;

    @Value("${dataplatform.dlorchestrationjob.queue.capacity}")
    private int queueCapacity;

    @Override
    public Callable<Boolean> getCallable(String jobArguments) {
        DLOrchestrationCallable.Builder builder = new DLOrchestrationCallable.Builder();
        builder.alertService(alertService)
                .appTimeLineWebAppAddress(appTimeLineWebAppAddress)
                .debugProcessorImpl(debugProcessorImpl)
                .dlOrchestrationJobTaskExecutor(taskExecutor)
                .featuresThreshold(featuresThreshold)
                .dbMetadataService(dbMetadataService)
                .modelCommandEntityMgr(modelCommandEntityMgr)
                .modelCommandLogService(modelCommandLogService)
                .modelCommandResultEntityMgr(modelCommandResultEntityMgr)
                .modelCommandStateEntityMgr(modelCommandStateEntityMgr)
                .modelingJobService(modelingJobService)
                .modelStepFinishProcessor(modelStepFinishProcessor)
                .modelStepOutputResultsProcessor(modelStepOutputResultsProcessor)
                .modelStepRetrieveMetadataProcessor(modelStepRetrieveMetadataProcessor)
                .modelStepYarnProcessor(modelStepYarnProcessor)
                .positiveEventFailThreshold(positiveEventFailThreshold)
                .positiveEventWarnThreshold(positiveEventWarnThreshold)
                .resourceManagerWebAppAddress(resourceManagerWebAppAddress)
                .rowFailThreshold(rowFailThreshold)
                .rowWarnThreshold(rowWarnThreshold)
                .waitTime(waitTime)
                .yarnConfiguration(yarnConfiguration)
                .modelDownloadFlagEntityMgr(modelDownloadFlagEntityMgr);
        return new DLOrchestrationCallable(builder);
    }

}
