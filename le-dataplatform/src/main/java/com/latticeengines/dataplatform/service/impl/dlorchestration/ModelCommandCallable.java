package com.latticeengines.dataplatform.service.impl.dlorchestration;

import java.util.List;
import java.util.concurrent.Callable;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;

import com.latticeengines.common.exposed.util.YarnUtils;
import com.latticeengines.dataplatform.entitymanager.ModelCommandEntityMgr;
import com.latticeengines.dataplatform.entitymanager.ModelCommandStateEntityMgr;
import com.latticeengines.dataplatform.exposed.exception.LedpException;
import com.latticeengines.dataplatform.service.JobService;
import com.latticeengines.dataplatform.service.ModelCommandLogService;
import com.latticeengines.dataplatform.service.ModelStepProcessor;
import com.latticeengines.domain.exposed.dataplatform.JobStatus;
import com.latticeengines.domain.exposed.dataplatform.dlorchestration.ModelCommand;
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
    
    private ModelStepProcessor modelStepProcessor;
    
    private ModelCommandLogService modelCommandLogService;  
    
    private ModelCommand modelCommand;
    
    public ModelCommandCallable(ModelCommand modelCommand, JobService jobService, ModelCommandEntityMgr modelCommandEntityMgr,
            ModelCommandStateEntityMgr modelCommandStateEntityMgr, ModelStepProcessor modelStepProcessor, 
            ModelCommandLogService modelCommandLogService) {
        this.modelCommand = modelCommand;
        this.jobService = jobService;
        this.modelCommandEntityMgr = modelCommandEntityMgr;
        this.modelCommandStateEntityMgr = modelCommandStateEntityMgr;
        this.modelStepProcessor = modelStepProcessor;
        this.modelCommandLogService = modelCommandLogService;
    }
    
    @Override
    public Long call() throws Exception {
        int result = SUCCESS;
        try {
            log.info("Begin work on modelcommand id:" + modelCommand.getPid()); // Need this line to associate modelCommandId with threadId in log4j output.
            executeWorkflow();
            log.info("End work on modelcommand id:" + modelCommand.getPid());            
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
            modelCommand.setCommandStatus(ModelCommandStatus.IN_PROGRESS);
            modelCommand.setModelCommandStep(ModelCommandStep.LOAD_DATA);
            modelCommandEntityMgr.update(modelCommand);            
                        
            executeYarnStep(ModelCommandStep.LOAD_DATA);            
        } else { // modelCommand IN_PROGRESS
            List<ModelCommandState> commandStates = modelCommandStateEntityMgr.findByModelCommandAndStep(modelCommand, modelCommand.getModelCommandStep());
            int successCount = 0;
            boolean jobFailed = false;
            
            for (ModelCommandState commandState : commandStates) {
                JobStatus jobStatus = jobService.getJobStatus(commandState.getYarnApplicationId());
                saveModelCommandStateFromJobStatus(commandState, jobStatus);
                
                if (jobStatus.getState().equals(FinalApplicationStatus.SUCCEEDED)) {
                    successCount++;
                } else if (jobStatus.getState().equals(FinalApplicationStatus.KILLED) || jobStatus.getState().equals(FinalApplicationStatus.FAILED)) {
                    jobFailed = true;
                } else if (jobStatus.getState().equals(FinalApplicationStatus.UNDEFINED) || YarnUtils.isPrempted(jobStatus.getDiagnostics())) {
                    // Job in progress.
                }
            }
            
            if (successCount == commandStates.size()) { // All jobs succeeded, move on to next step
                handleAllJobsSucceeded();
            } else if (jobFailed) {
                handleJobFailed();
            } else {
                // Do nothing; job(s) in progress.
            }                       
        }
    }
    
    private void handleAllJobsSucceeded() {
        modelCommandLogService.logCompleteStep(modelCommand, modelCommand.getModelCommandStep(), ModelCommandStatus.SUCCESS);
        
        ModelCommandStep nextStep = modelCommand.getModelCommandStep().getNextStep();
        modelCommand.setModelCommandStep(nextStep);
        
        if (nextStep.equals(ModelCommandStep.FINISH)) {
            finish();
        } else {
            executeYarnStep(nextStep); 
        }                         

        modelCommandEntityMgr.update(modelCommand);     
        
    }
   
    private void handleJobFailed() {     
        modelCommandLogService.logCompleteStep(modelCommand, modelCommand.getModelCommandStep(), ModelCommandStatus.FAIL);
        
        modelCommand.setCommandStatus(ModelCommandStatus.FAIL);                
        modelCommandEntityMgr.update(modelCommand);        
    }
    
    private void executeYarnStep(ModelCommandStep step) {
        modelCommandLogService.logBeginStep(modelCommand, step);
        
        List<ApplicationId> appIds = modelStepProcessor.executeYarnStep(modelCommand.getDeploymentExternalId(), step, modelCommand.getCommandParameters());       
        for (ApplicationId appId : appIds) {
            JobStatus jobStatus = jobService.getJobStatus(appId.toString());
            
            ModelCommandState commandState = new ModelCommandState(modelCommand, step);
            commandState.setYarnApplicationId(appId.toString());                                   
            saveModelCommandStateFromJobStatus(commandState, jobStatus);            
        }               
    }       
    
    private void finish() {
        modelCommandLogService.logBeginStep(modelCommand, ModelCommandStep.FINISH);            

        ModelCommandState commandState = new ModelCommandState(modelCommand, ModelCommandStep.FINISH);                                                                                                                  
        modelCommandStateEntityMgr.create(commandState);        
        
        modelCommandLogService.logCompleteStep(modelCommand, ModelCommandStep.FINISH, ModelCommandStatus.SUCCESS);
        modelCommand.setCommandStatus(ModelCommandStatus.SUCCESS);
    }
    
    private void saveModelCommandStateFromJobStatus(ModelCommandState commandState, JobStatus jobStatus) {
        commandState.setStatus(jobStatus.getState());
        commandState.setProgress(jobStatus.getProgress());
        commandState.setDiagnostics(jobStatus.getDiagnostics());
        commandState.setTrackingUrl(jobStatus.getTrackingUrl());
        commandState.setElapsedTimeInMillis(System.currentTimeMillis()-jobStatus.getStartTime());                
        modelCommandStateEntityMgr.createOrUpdate(commandState);
    }
}       
