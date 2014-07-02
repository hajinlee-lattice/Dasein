package com.latticeengines.dataplatform.service.impl.dlorchestration;

import java.util.Date;
import java.util.List;
import java.util.concurrent.Callable;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;

import com.latticeengines.common.exposed.util.YarnUtils;
import com.latticeengines.dataplatform.entitymanager.ModelCommandEntityMgr;
import com.latticeengines.dataplatform.entitymanager.ModelCommandResultEntityMgr;
import com.latticeengines.dataplatform.entitymanager.ModelCommandStateEntityMgr;
import com.latticeengines.dataplatform.exposed.exception.LedpException;
import com.latticeengines.dataplatform.service.JobService;
import com.latticeengines.dataplatform.service.dlorchestration.ModelCommandLogService;
import com.latticeengines.dataplatform.service.dlorchestration.ModelStepProcessor;
import com.latticeengines.dataplatform.service.dlorchestration.ModelStepYarnProcessor;
import com.latticeengines.domain.exposed.dataplatform.JobStatus;
import com.latticeengines.domain.exposed.dataplatform.dlorchestration.ModelCommand;
import com.latticeengines.domain.exposed.dataplatform.dlorchestration.ModelCommandResult;
import com.latticeengines.domain.exposed.dataplatform.dlorchestration.ModelCommandState;
import com.latticeengines.domain.exposed.dataplatform.dlorchestration.ModelCommandStatus;
import com.latticeengines.domain.exposed.dataplatform.dlorchestration.ModelCommandStep;

public class ModelCommandCallable implements Callable<Long> {

    private static final Log log = LogFactory.getLog(ModelCommandCallable.class);

    private static final int SUCCESS = 0;
    private static final int FAIL = -1;

    private JobService jobService;

    private ModelCommandEntityMgr modelCommandEntityMgr;

    private ModelCommandStateEntityMgr modelCommandStateEntityMgr;

    private ModelStepYarnProcessor modelStepYarnProcessor;

    private ModelCommandLogService modelCommandLogService;

    private ModelCommandResultEntityMgr modelCommandResultEntityMgr;

    private ModelStepProcessor modelStepFinishProcessor;

    private ModelStepProcessor modelStepOutputResultsProcessor;

    ModelStepProcessor modelStepRetrieveMetadataProcessor;

    private ModelCommand modelCommand;

    public ModelCommandCallable(ModelCommand modelCommand, JobService jobService,
            ModelCommandEntityMgr modelCommandEntityMgr, ModelCommandStateEntityMgr modelCommandStateEntityMgr,
            ModelStepYarnProcessor modelStepYarnProcessor, ModelCommandLogService modelCommandLogService,
            ModelCommandResultEntityMgr modelCommandResultEntityMgr, ModelStepProcessor modelStepFinishProcessor,
            ModelStepProcessor modelStepOutputResultsProcessor, ModelStepProcessor modelStepRetrieveMetadataProcessor) {
        this.modelCommand = modelCommand;
        this.jobService = jobService;
        this.modelCommandEntityMgr = modelCommandEntityMgr;
        this.modelCommandStateEntityMgr = modelCommandStateEntityMgr;
        this.modelStepYarnProcessor = modelStepYarnProcessor;
        this.modelCommandLogService = modelCommandLogService;
        this.modelCommandResultEntityMgr = modelCommandResultEntityMgr;
        this.modelStepOutputResultsProcessor = modelStepOutputResultsProcessor;
        this.modelStepFinishProcessor = modelStepFinishProcessor;
        this.modelStepRetrieveMetadataProcessor = modelStepRetrieveMetadataProcessor;
    }

    @Override
    public Long call() throws Exception {
        int result = SUCCESS;
        try {
            log.info("Begin scheduled work on " + ModelCommandLogServiceImpl.MODELCOMMAND_ID_LOG_PREFIX + ":"
                    + modelCommand.getPid()); // Need this line to associate
                                              // modelCommandId with threadId in
                                              // log4j output.
            executeWorkflow();
            log.info("End scheduled work on " + ModelCommandLogServiceImpl.MODELCOMMAND_ID_LOG_PREFIX + ":"
                    + modelCommand.getPid());
        } catch (LedpException e) {
            result = FAIL;
            modelCommandLogService.logLedpException(modelCommand, e);
        } catch (Exception e) {
            result = FAIL;
            modelCommandLogService.logException(modelCommand, e);
        } finally {
            if (result == FAIL) {
                handleJobFailed();
            }
        }
        return modelCommand.getPid();
    }

    private void executeWorkflow() {
        if (modelCommand.isNew()) {
            Date now = new Date();
            modelCommandResultEntityMgr.create(new ModelCommandResult(modelCommand, now, now, ModelCommandStatus.IN_PROGRESS));

            modelCommand.setCommandStatus(ModelCommandStatus.IN_PROGRESS);

            ModelCommandParameters commandParameters = new ModelCommandParameters(modelCommand.getCommandParameters());
            executeStep(modelStepRetrieveMetadataProcessor, ModelCommandStep.RETRIEVE_METADATA, commandParameters);

            modelCommand.setModelCommandStep(ModelCommandStep.LOAD_DATA);
            modelCommandEntityMgr.update(modelCommand);
            executeYarnStep(ModelCommandStep.LOAD_DATA, commandParameters);
        } else { // modelCommand IN_PROGRESS
            List<ModelCommandState> commandStates = modelCommandStateEntityMgr.findByModelCommandAndStep(modelCommand,
                    modelCommand.getModelCommandStep());
            int successCount = 0;
            boolean jobFailed = false;

            for (ModelCommandState commandState : commandStates) {
                JobStatus jobStatus = jobService.getJobStatus(commandState.getYarnApplicationId());
                saveModelCommandStateFromJobStatus(commandState, jobStatus);

                if (jobStatus.getState().equals(FinalApplicationStatus.SUCCEEDED)) {
                    successCount++;
                } else if (jobStatus.getState().equals(FinalApplicationStatus.KILLED)
                        || jobStatus.getState().equals(FinalApplicationStatus.FAILED)) {
                    jobFailed = true;
                } else if (jobStatus.getState().equals(FinalApplicationStatus.UNDEFINED)
                        || YarnUtils.isPrempted(jobStatus.getDiagnostics())) {
                    // Job in progress.
                }
            }

            if (successCount == commandStates.size()) { // All jobs succeeded,
                                                        // move on to next step
                handleAllJobsSucceeded();
            } else if (jobFailed) {
                handleJobFailed();
            } else {
                // Do nothing; job(s) in progress.
            }
        }
    }

    private void handleAllJobsSucceeded() {
        modelCommandLogService.logCompleteStep(modelCommand, modelCommand.getModelCommandStep(),
                ModelCommandStatus.SUCCESS);

        ModelCommandStep nextStep = modelCommand.getModelCommandStep().getNextStep();
        modelCommand.setModelCommandStep(nextStep);

        ModelCommandParameters commandParameters = new ModelCommandParameters(modelCommand.getCommandParameters());
        if (nextStep.equals(ModelCommandStep.OUTPUT_COMMAND_RESULTS)) {
            executeStep(modelStepOutputResultsProcessor, ModelCommandStep.OUTPUT_COMMAND_RESULTS, commandParameters);
            executeStep(modelStepFinishProcessor, ModelCommandStep.FINISH, commandParameters);
        } else {
            executeYarnStep(nextStep, commandParameters);
        }
    }

    private void handleJobFailed() {
        modelCommandLogService.logCompleteStep(modelCommand, modelCommand.getModelCommandStep(),
                ModelCommandStatus.FAIL);

        ModelCommandResult result = modelCommandResultEntityMgr.findByModelCommand(modelCommand);
        result.setEndTime(new Date());
        result.setProcessStatus(ModelCommandStatus.FAIL);
        modelCommandResultEntityMgr.update(result);

        modelCommand.setCommandStatus(ModelCommandStatus.FAIL);
        modelCommandEntityMgr.update(modelCommand);
    }

    private void executeYarnStep(ModelCommandStep step, ModelCommandParameters commandParameters) {
        modelCommandLogService.logBeginStep(modelCommand, step);

        List<ApplicationId> appIds = modelStepYarnProcessor.executeYarnStep(modelCommand.getDeploymentExternalId(),
                step, commandParameters);
        for (ApplicationId appId : appIds) {
            JobStatus jobStatus = jobService.getJobStatus(appId.toString());

            ModelCommandState commandState = new ModelCommandState(modelCommand, step);
            commandState.setYarnApplicationId(appId.toString());
            saveModelCommandStateFromJobStatus(commandState, jobStatus);
        }
        modelCommandEntityMgr.update(modelCommand);
    }

    private void executeStep(ModelStepProcessor processor, ModelCommandStep step,
            ModelCommandParameters commandParameters) {
        long start = System.currentTimeMillis();
        modelCommandLogService.logBeginStep(modelCommand, step);
        ModelCommandState commandState = new ModelCommandState(modelCommand, step);
        commandState.setStatus(FinalApplicationStatus.UNDEFINED);
        modelCommandStateEntityMgr.create(commandState);

        processor.executeStep(modelCommand, commandParameters);

        commandState.setElapsedTimeInMillis(System.currentTimeMillis() - start);
        commandState.setStatus(FinalApplicationStatus.SUCCEEDED);
        modelCommandStateEntityMgr.update(commandState);
        modelCommandLogService.logCompleteStep(modelCommand, step, ModelCommandStatus.SUCCESS);

        modelCommand.setModelCommandStep(step);
        modelCommandEntityMgr.update(modelCommand);
    }

    private void saveModelCommandStateFromJobStatus(ModelCommandState commandState, JobStatus jobStatus) {
        commandState.setStatus(jobStatus.getState());
        commandState.setProgress(jobStatus.getProgress());
        commandState.setDiagnostics(jobStatus.getDiagnostics());
        commandState.setTrackingUrl(jobStatus.getTrackingUrl());
        commandState.setElapsedTimeInMillis(System.currentTimeMillis() - jobStatus.getStartTime());
        modelCommandStateEntityMgr.createOrUpdate(commandState);
    }

}
