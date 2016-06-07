package com.latticeengines.dataplatform.service.impl.dlorchestration;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.springframework.core.task.AsyncTaskExecutor;

import com.google.common.base.Joiner;
import com.latticeengines.dataplatform.entitymanager.ModelCommandEntityMgr;
import com.latticeengines.dataplatform.entitymanager.ModelCommandResultEntityMgr;
import com.latticeengines.dataplatform.entitymanager.ModelCommandStateEntityMgr;
import com.latticeengines.dataplatform.exposed.service.MetadataService;
import com.latticeengines.dataplatform.service.dlorchestration.ModelCommandLogService;
import com.latticeengines.dataplatform.service.dlorchestration.ModelStepProcessor;
import com.latticeengines.dataplatform.service.dlorchestration.ModelStepYarnProcessor;
import com.latticeengines.dataplatform.service.modeling.ModelingJobService;
import com.latticeengines.domain.exposed.dataplatform.dlorchestration.ModelCommand;
import com.latticeengines.monitor.exposed.alerts.service.AlertService;

public class DLOrchestrationCallable implements Callable<Boolean> {

    private static final Log log = LogFactory.getLog(DLOrchestrationCallable.class);

    private AsyncTaskExecutor dlOrchestrationJobTaskExecutor;
    private ModelCommandEntityMgr modelCommandEntityMgr;
    private ModelingJobService modelingJobService;
    private ModelCommandStateEntityMgr modelCommandStateEntityMgr;
    private ModelStepYarnProcessor modelStepYarnProcessor;
    private ModelCommandLogService modelCommandLogService;
    private ModelCommandResultEntityMgr modelCommandResultEntityMgr;
    private ModelStepProcessor modelStepFinishProcessor;
    private ModelStepProcessor modelStepOutputResultsProcessor;
    private ModelStepProcessor modelStepRetrieveMetadataProcessor;
    private AlertService alertService;
    private DebugProcessorImpl debugProcessorImpl;
    private int waitTime = 180;
    private Configuration yarnConfiguration;
    private String resourceManagerWebAppAddress;
    private String appTimeLineWebAppAddress;
    private int rowFailThreshold;
    private int rowWarnThreshold;
    private int positiveEventFailThreshold;
    private int positiveEventWarnThreshold;
    private int featuresThreshold;
    private MetadataService metadataService;

    public DLOrchestrationCallable(Builder builder) {
        this.dlOrchestrationJobTaskExecutor = builder.dlOrchestrationJobTaskExecutor;
        this.modelCommandEntityMgr = builder.modelCommandEntityMgr;
        this.modelingJobService = builder.modelingJobService;
        this.modelCommandStateEntityMgr = builder.modelCommandStateEntityMgr;
        this.modelStepYarnProcessor = builder.modelStepYarnProcessor;
        this.modelCommandLogService = builder.modelCommandLogService;
        this.modelCommandResultEntityMgr = builder.modelCommandResultEntityMgr;
        this.modelStepFinishProcessor = builder.modelStepFinishProcessor;
        this.modelStepOutputResultsProcessor = builder.modelStepOutputResultsProcessor;
        this.modelStepRetrieveMetadataProcessor = builder.modelStepRetrieveMetadataProcessor;
        this.alertService = builder.alertService;
        this.debugProcessorImpl = builder.debugProcessorImpl;
        this.waitTime = builder.waitTime;
        this.yarnConfiguration = builder.yarnConfiguration;
        this.resourceManagerWebAppAddress = builder.resourceManagerWebAppAddress;
        this.appTimeLineWebAppAddress = builder.appTimeLineWebAppAddress;
        this.rowFailThreshold = builder.rowFailThreshold;
        this.rowWarnThreshold = builder.rowWarnThreshold;
        this.positiveEventFailThreshold = builder.positiveEventFailThreshold;
        this.positiveEventWarnThreshold = builder.positiveEventWarnThreshold;
        this.featuresThreshold = builder.featuresThreshold;
        this.metadataService = builder.metadataService;
    }

    @Override
    public Boolean call() throws Exception {
        List<Future<Long>> futures = new ArrayList<>();
        List<ModelCommand> modelCommands = modelCommandEntityMgr.getNewAndInProgress();
        String modelCommandsStr = Joiner.on(",").join(modelCommands);

        if (log.isDebugEnabled()) {
            log.debug("Begin processing " + modelCommands.size() + " model commands: "
                    + modelCommandsStr);
        }

        for (ModelCommand modelCommand : modelCommands) {
            ModelCommandCallable.Builder builder = new ModelCommandCallable.Builder();
            builder.modelCommand(modelCommand) //
                    .yarnConfiguration(yarnConfiguration) //
                    .modelingJobService(modelingJobService) //
                    .modelCommandEntityMgr(modelCommandEntityMgr) //
                    .modelCommandStateEntityMgr(modelCommandStateEntityMgr) //
                    .modelStepYarnProcessor(modelStepYarnProcessor) //
                    .modelCommandLogService(modelCommandLogService) //
                    .modelCommandResultEntityMgr(modelCommandResultEntityMgr) //
                    .modelStepFinishProcessor(modelStepFinishProcessor) //
                    .modelStepOutputResultsProcessor(modelStepOutputResultsProcessor) //
                    .modelStepRetrieveMetadataProcessor(modelStepRetrieveMetadataProcessor) //
                    .debugProcessorImpl(debugProcessorImpl) //
                    .alertService(alertService) //
                    .resourceManagerWebAppAddress(resourceManagerWebAppAddress) //
                    .appTimeLineWebAppAddress(appTimeLineWebAppAddress) //
                    .rowFailThreshold(rowFailThreshold) //
                    .rowWarnThreshold(rowWarnThreshold) //
                    .positiveEventFailThreshold(positiveEventFailThreshold) //
                    .positiveEventWarnThreshold(positiveEventWarnThreshold) //
                    .featuresThreshold(featuresThreshold)
                    .metadataService(metadataService);
            futures.add(dlOrchestrationJobTaskExecutor.submit(new ModelCommandCallable(builder)));
        }
        for (Future<Long> future : futures) {
            try {
                future.get(waitTime, TimeUnit.SECONDS);
            } catch (Exception e) {
                // ModelCommandCallable is responsible for consuming any
                // exceptions while processing
                // An exception here indicates a problem outside of the
                // workflow.
                log.fatal(e.getMessage(), e);
            }
        }

        if (log.isDebugEnabled()) {
            log.debug("Finished processing " + modelCommands.size() + " model commands: "
                    + modelCommandsStr);
        }
        return true;
    }

    public static class Builder {
        private AsyncTaskExecutor dlOrchestrationJobTaskExecutor;
        private ModelCommandEntityMgr modelCommandEntityMgr;
        private ModelingJobService modelingJobService;
        private ModelCommandStateEntityMgr modelCommandStateEntityMgr;
        private ModelStepYarnProcessor modelStepYarnProcessor;
        private ModelCommandLogService modelCommandLogService;
        private ModelCommandResultEntityMgr modelCommandResultEntityMgr;
        private ModelStepProcessor modelStepFinishProcessor;
        private ModelStepProcessor modelStepOutputResultsProcessor;
        private ModelStepProcessor modelStepRetrieveMetadataProcessor;
        private AlertService alertService;
        private DebugProcessorImpl debugProcessorImpl;
        private int waitTime = 180;
        private Configuration yarnConfiguration;
        private String resourceManagerWebAppAddress;
        private String appTimeLineWebAppAddress;
        private int rowFailThreshold;
        private int rowWarnThreshold;
        private int positiveEventFailThreshold;
        private int positiveEventWarnThreshold;
        private int featuresThreshold;
        private MetadataService metadataService;

        public Builder() {

        }

        public Builder dlOrchestrationJobTaskExecutor(
                AsyncTaskExecutor dlOrchestrationJobTaskExecutor) {
            this.dlOrchestrationJobTaskExecutor = dlOrchestrationJobTaskExecutor;
            return this;
        }

        public Builder modelCommandEntityMgr(ModelCommandEntityMgr modelCommandEntityMgr) {
            this.modelCommandEntityMgr = modelCommandEntityMgr;
            return this;
        }

        public Builder modelingJobService(ModelingJobService modelingJobService) {
            this.modelingJobService = modelingJobService;
            return this;
        }

        public Builder modelCommandStateEntityMgr(
                ModelCommandStateEntityMgr modelCommandStateEntityMgr) {
            this.modelCommandStateEntityMgr = modelCommandStateEntityMgr;
            return this;
        }

        public Builder modelStepYarnProcessor(ModelStepYarnProcessor modelStepYarnProcessor) {
            this.modelStepYarnProcessor = modelStepYarnProcessor;
            return this;
        }

        public Builder modelCommandLogService(ModelCommandLogService modelCommandLogService) {
            this.modelCommandLogService = modelCommandLogService;
            return this;
        }

        public Builder modelCommandResultEntityMgr(
                ModelCommandResultEntityMgr modelCommandResultEntityMgr) {
            this.modelCommandResultEntityMgr = modelCommandResultEntityMgr;
            return this;
        }

        public Builder modelStepFinishProcessor(ModelStepProcessor modelStepFinishProcessor) {
            this.modelStepFinishProcessor = modelStepFinishProcessor;
            return this;
        }

        public Builder modelStepOutputResultsProcessor(
                ModelStepProcessor modelStepOutputResultsProcessor) {
            this.modelStepOutputResultsProcessor = modelStepOutputResultsProcessor;
            return this;
        }

        public Builder modelStepRetrieveMetadataProcessor(
                ModelStepProcessor modelStepRetrieveMetadataProcessor) {
            this.modelStepRetrieveMetadataProcessor = modelStepRetrieveMetadataProcessor;
            return this;
        }

        public Builder alertService(AlertService alertService) {
            this.alertService = alertService;
            return this;
        }

        public Builder debugProcessorImpl(DebugProcessorImpl debugProcessorImpl) {
            this.debugProcessorImpl = debugProcessorImpl;
            return this;
        }

        public Builder waitTime(int waitTime) {
            this.waitTime = waitTime;
            return this;
        }

        public Builder yarnConfiguration(Configuration yarnConfiguration) {
            this.yarnConfiguration = yarnConfiguration;
            return this;
        }

        public Builder resourceManagerWebAppAddress(String resourceManagerWebAppAddress) {
            this.resourceManagerWebAppAddress = resourceManagerWebAppAddress;
            return this;
        }

        public Builder appTimeLineWebAppAddress(String appTimeLineWebAppAddress) {
            this.appTimeLineWebAppAddress = appTimeLineWebAppAddress;
            return this;
        }

        public Builder rowFailThreshold(int rowFailThreshold) {
            this.rowFailThreshold = rowFailThreshold;
            return this;
        }

        public Builder rowWarnThreshold(int rowWarnThreshold) {
            this.rowWarnThreshold = rowWarnThreshold;
            return this;
        }

        public Builder positiveEventFailThreshold(int positiveEventFailThreshold) {
            this.positiveEventFailThreshold = positiveEventFailThreshold;
            return this;
        }

        public Builder positiveEventWarnThreshold(int positiveEventWarnThreshold) {
            this.positiveEventWarnThreshold = positiveEventWarnThreshold;
            return this;
        }

        public Builder featuresThreshold(int featuresThreshold) {
            this.featuresThreshold = featuresThreshold;
            return this;
        }

        public Builder metadataService(MetadataService metadataService) {
            this.metadataService = metadataService;
            return this;
        }

    }

}
