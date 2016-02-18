package com.latticeengines.dataplatform.service.impl.dlorchestration;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.quartz.DisallowConcurrentExecution;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.springframework.core.task.AsyncTaskExecutor;
import org.springframework.scheduling.quartz.QuartzJobBean;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.google.common.base.Joiner;
import com.latticeengines.dataplatform.entitymanager.ModelCommandEntityMgr;
import com.latticeengines.dataplatform.entitymanager.ModelCommandResultEntityMgr;
import com.latticeengines.dataplatform.entitymanager.ModelCommandStateEntityMgr;
import com.latticeengines.dataplatform.exposed.service.MetadataService;
import com.latticeengines.dataplatform.service.dlorchestration.DLOrchestrationService;
import com.latticeengines.dataplatform.service.dlorchestration.ModelCommandLogService;
import com.latticeengines.dataplatform.service.dlorchestration.ModelStepProcessor;
import com.latticeengines.dataplatform.service.dlorchestration.ModelStepYarnProcessor;
import com.latticeengines.dataplatform.service.modeling.ModelingJobService;
import com.latticeengines.domain.exposed.dataplatform.dlorchestration.ModelCommand;
import com.latticeengines.monitor.exposed.alerts.service.AlertService;

@DisallowConcurrentExecution
@Component("dlOrchestrationJob")
public class DLOrchestrationServiceImpl extends QuartzJobBean implements DLOrchestrationService {

    private static final Log log = LogFactory.getLog(DLOrchestrationServiceImpl.class);

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

    @Override
    protected void executeInternal(JobExecutionContext context) throws JobExecutionException {
        run(context);
    }

    @Override
    @Transactional(value = "dlorchestration", propagation = Propagation.REQUIRED)
    public void run(JobExecutionContext context) throws JobExecutionException {
        List<Future<Long>> futures = new ArrayList<>();
        List<ModelCommand> modelCommands = modelCommandEntityMgr.getNewAndInProgress();
        String modelCommandsStr = Joiner.on(",").join(modelCommands);

        if (log.isDebugEnabled()) {
            log.debug("Begin processing " + modelCommands.size() + " model commands: " + modelCommandsStr);
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
            log.debug("Finished processing " + modelCommands.size() + " model commands: " + modelCommandsStr);
        }

    }

    public AsyncTaskExecutor getDlOrchestrationJobTaskExecutor() {
        return dlOrchestrationJobTaskExecutor;
    }

    public void setDlOrchestrationJobTaskExecutor(AsyncTaskExecutor dlOrchestrationJobTaskExecutor) {
        this.dlOrchestrationJobTaskExecutor = dlOrchestrationJobTaskExecutor;
    }

    public ModelCommandEntityMgr getModelCommandEntityMgr() {
        return modelCommandEntityMgr;
    }

    public void setModelCommandEntityMgr(ModelCommandEntityMgr modelCommandEntityMgr) {
        this.modelCommandEntityMgr = modelCommandEntityMgr;
    }

    public ModelingJobService getModelingJobService() {
        return modelingJobService;
    }

    public void setModelingJobService(ModelingJobService modelingJobService) {
        this.modelingJobService = modelingJobService;
    }

    public ModelCommandStateEntityMgr getModelCommandStateEntityMgr() {
        return modelCommandStateEntityMgr;
    }

    public void setModelCommandStateEntityMgr(ModelCommandStateEntityMgr modelCommandStateEntityMgr) {
        this.modelCommandStateEntityMgr = modelCommandStateEntityMgr;
    }

    public ModelStepYarnProcessor getModelStepYarnProcessor() {
        return modelStepYarnProcessor;
    }

    public void setModelStepYarnProcessor(ModelStepYarnProcessor modelStepYarnProcessor) {
        this.modelStepYarnProcessor = modelStepYarnProcessor;
    }

    public ModelCommandLogService getModelCommandLogService() {
        return modelCommandLogService;
    }

    public void setModelCommandLogService(ModelCommandLogService modelCommandLogService) {
        this.modelCommandLogService = modelCommandLogService;
    }

    public ModelCommandResultEntityMgr getModelCommandResultEntityMgr() {
        return modelCommandResultEntityMgr;
    }

    public void setModelCommandResultEntityMgr(ModelCommandResultEntityMgr modelCommandResultEntityMgr) {
        this.modelCommandResultEntityMgr = modelCommandResultEntityMgr;
    }

    public ModelStepProcessor getModelStepFinishProcessor() {
        return modelStepFinishProcessor;
    }

    public void setModelStepFinishProcessor(ModelStepProcessor modelStepFinishProcessor) {
        this.modelStepFinishProcessor = modelStepFinishProcessor;
    }

    public ModelStepProcessor getModelStepOutputResultsProcessor() {
        return modelStepOutputResultsProcessor;
    }

    public void setModelStepOutputResultsProcessor(ModelStepProcessor modelStepOutputResultsProcessor) {
        this.modelStepOutputResultsProcessor = modelStepOutputResultsProcessor;
    }

    public ModelStepProcessor getModelStepRetrieveMetadataProcessor() {
        return modelStepRetrieveMetadataProcessor;
    }

    public void setModelStepRetrieveMetadataProcessor(ModelStepProcessor modelStepRetrieveMetadataProcessor) {
        this.modelStepRetrieveMetadataProcessor = modelStepRetrieveMetadataProcessor;
    }

    public int getWaitTime() {
        return waitTime;
    }

    public void setWaitTime(int waitTime) {
        this.waitTime = waitTime;
    }

    public DebugProcessorImpl getDebugProcessorImpl() {
        return debugProcessorImpl;
    }

    public void setDebugProcessorImpl(DebugProcessorImpl debugProcessorImpl) {
        this.debugProcessorImpl = debugProcessorImpl;
    }

    public Configuration getYarnConfiguration() {
        return yarnConfiguration;
    }

    public void setYarnConfiguration(Configuration yarnConfiguration) {
        this.yarnConfiguration = yarnConfiguration;
    }

    public AlertService getAlertService() {
        return alertService;
    }

    public void setAlertService(AlertService alertService) {
        this.alertService = alertService;
    }

    public String getResourceManagerWebAppAddress() {
        return resourceManagerWebAppAddress;
    }

    public void setResourceManagerWebAppAddress(String resourceManagerWebAppAddress) {
        this.resourceManagerWebAppAddress = resourceManagerWebAppAddress;
    }

    public String getAppTimeLineWebAppAddress() {
        return appTimeLineWebAppAddress;
    }

    public void setAppTimeLineWebAppAddress(String appTimeLineWebAppAddress) {
        this.appTimeLineWebAppAddress = appTimeLineWebAppAddress;
    }

    public int getRowFailThreshold() {
        return rowFailThreshold;
    }

    public void setRowFailThreshold(int rowFailThreshold) {
        this.rowFailThreshold = rowFailThreshold;
    }

    public int getRowWarnThreshold() {
        return rowWarnThreshold;
    }

    public void setRowWarnThreshold(int rowWarnThreshold) {
        this.rowWarnThreshold = rowWarnThreshold;
    }

    public int getPositiveEventFailThreshold() {
        return positiveEventFailThreshold;
    }

    public void setPositiveEventFailThreshold(int positiveEventFailThreshold) {
        this.positiveEventFailThreshold = positiveEventFailThreshold;
    }

    public int getPositiveEventWarnThreshold() {
        return positiveEventWarnThreshold;
    }

    public void setPositiveEventWarnThreshold(int positiveEventWarnThreshold) {
        this.positiveEventWarnThreshold = positiveEventWarnThreshold;
    }

    public void setFeaturesThreshold(int featuresThreshold) {
        this.featuresThreshold = featuresThreshold;
    }

    public int getFeaturesThreshold() {
        return this.featuresThreshold;
    }

    public MetadataService getMetadataService() {
        return metadataService;
    }

    public void setMetadataService(MetadataService metadataService) {
        this.metadataService = metadataService;
    }
}
