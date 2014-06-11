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

public class ModelCommandCallable implements Callable<Integer> {
    
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
    public Integer call() throws Exception {
        int result = SUCCESS;
        try {
            log.info("Begin work on modelcommand id:" + modelCommand.getId()); // Need this line to associate modelCommandId with threadId in log4j output.
            executeWorkflow();
            log.info("End work on modelcommand id:" + modelCommand.getId());            
        } catch (LedpException e) {
            result = FAIL;
            modelCommandLogService.logLedpException(modelCommand.getCommandId(), e);
        } catch (Exception e) {
            result = FAIL;
            modelCommandLogService.logException(modelCommand.getCommandId(), e);
        } finally {
            if (result == FAIL) {
                handleJobFailed();                                
            }
        }
        return modelCommand.getCommandId();
    }
    
    private void executeWorkflow() {             
        if (modelCommand.isNew()) {                          
            modelCommand.setCommandStatus(ModelCommandStatus.IN_PROGRESS);
            //modelCommandEntityMgr.post(modelCommand);            
                        
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
        modelCommandLogService.logCompleteStep(modelCommand.getCommandId(), modelCommand.getModelCommandStep(), ModelCommandStatus.SUCCESS);
        
        ModelCommandStep nextStep = modelCommand.getModelCommandStep().getNextStep();
        modelCommand.setModelCommandStep(nextStep);
        
        if (nextStep.equals(ModelCommandStep.SEND_JSON)) {
            executeJSONStep();
        } else {
            executeYarnStep(nextStep); 
        }                         

        //modelCommandEntityMgr.post(modelCommand);     
        
    }
   
    private void handleJobFailed() {     
        modelCommandLogService.logCompleteStep(modelCommand.getCommandId(), modelCommand.getModelCommandStep(), ModelCommandStatus.FAIL);
        
        modelCommand.setCommandStatus(ModelCommandStatus.FAIL);                
        ///modelCommandEntityMgr.post(modelCommand);        
    }
    
    private void executeYarnStep(ModelCommandStep step) {
        modelCommandLogService.logBeginStep(modelCommand.getCommandId(), step);
        
        List<ApplicationId> appIds = modelStepProcessor.executeYarnStep(modelCommand.getDeploymentExternalId(), step, modelCommand.getCommandParameters());       
        for (ApplicationId appId : appIds) {
            JobStatus jobStatus = jobService.getJobStatus(appId.toString());
            
            ModelCommandState commandState = new ModelCommandState();
            commandState.setYarnApplicationId(appId.toString());
            commandState.setCommandId(modelCommand.getCommandId());
            commandState.setModelCommandStep(step);
            
            saveModelCommandStateFromJobStatus(commandState, jobStatus);            
        }               
    }       
    
    private void executeJSONStep() {
        modelCommandLogService.logBeginStep(modelCommand.getCommandId(), ModelCommandStep.SEND_JSON);            

        ModelCommandState commandState = new ModelCommandState();                   
        commandState.setCommandId(modelCommand.getCommandId());
        commandState.setModelCommandStep(ModelCommandStep.SEND_JSON);                                                                               
        ///modelCommandStateEntityMgr.post(commandState);        
             
        modelStepProcessor.executeJsonStep(modelCommand.getDeploymentExternalId(), modelCommand.getCommandId(), modelCommand.getCommandParameters());
        
        modelCommandLogService.logCompleteStep(modelCommand.getCommandId(), ModelCommandStep.SEND_JSON, ModelCommandStatus.SUCCESS);
        modelCommand.setCommandStatus(ModelCommandStatus.SUCCESS);
    }
    
    private void saveModelCommandStateFromJobStatus(ModelCommandState commandState, JobStatus jobStatus) {
        commandState.setProgress(jobStatus.getProgress());
        commandState.setDiagnostics(jobStatus.getDiagnostics());
        commandState.setTrackingUrl(jobStatus.getTrackingUrl());
        commandState.setElapsedTimeInMillis(System.currentTimeMillis()-jobStatus.getStartTime());                
        ///modelCommandStateEntityMgr.post(commandState);
    }
}       
