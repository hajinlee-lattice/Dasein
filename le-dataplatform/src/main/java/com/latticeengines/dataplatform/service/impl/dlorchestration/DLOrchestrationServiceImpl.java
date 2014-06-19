package com.latticeengines.dataplatform.service.impl.dlorchestration;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
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
import com.latticeengines.dataplatform.service.JobService;
import com.latticeengines.dataplatform.service.dlorchestration.DLOrchestrationService;
import com.latticeengines.dataplatform.service.dlorchestration.ModelCommandLogService;
import com.latticeengines.dataplatform.service.dlorchestration.ModelStepPostProcessor;
import com.latticeengines.dataplatform.service.dlorchestration.ModelStepYarnProcessor;
import com.latticeengines.domain.exposed.dataplatform.dlorchestration.ModelCommand;

@DisallowConcurrentExecution
@Component("dlOrchestrationJob")
public class DLOrchestrationServiceImpl extends QuartzJobBean implements DLOrchestrationService {
    
    private static final Log log = LogFactory.getLog(DLOrchestrationServiceImpl.class);
    
    private AsyncTaskExecutor dlOrchestrationJobTaskExecutor;
    
    private ModelCommandEntityMgr modelCommandEntityMgr;
         
    private JobService jobService;  
   
    private ModelCommandStateEntityMgr modelCommandStateEntityMgr;
    
    private ModelStepYarnProcessor modelStepYarnProcessor;
    
    private ModelCommandLogService modelCommandLogService;
    
    private ModelCommandResultEntityMgr modelCommandResultEntityMgr;
    
    private ModelStepPostProcessor modelStepFinishProcessor;
    
    private ModelStepPostProcessor modelStepOutputResultsProcessor;
    
    private int waitTime = 180;
    
    @Override
    protected void executeInternal(JobExecutionContext context) throws JobExecutionException {
        run(context);
    }

    @Override
    @Transactional(value="dlorchestration", propagation = Propagation.REQUIRED)    
    public void run(JobExecutionContext context) throws JobExecutionException {
        List<Future<Long>> futures = new ArrayList<>();
        List<ModelCommand> modelCommands = modelCommandEntityMgr.getNewAndInProgress();
        String modelCommandsStr = Joiner.on(",").join(modelCommands);
        log.debug("Begin processing " + modelCommands.size() + " model commands: " + modelCommandsStr);
        for (ModelCommand modelCommand : modelCommands) {
            futures.add(dlOrchestrationJobTaskExecutor.submit(new ModelCommandCallable(modelCommand, 
                    jobService, modelCommandEntityMgr, modelCommandStateEntityMgr, modelStepYarnProcessor,
                    modelCommandLogService, modelCommandResultEntityMgr, modelStepFinishProcessor, modelStepOutputResultsProcessor)));
        }
        for (Future<Long> future : futures) {
            try {
                future.get(waitTime, TimeUnit.SECONDS);                
            } catch (Exception e) {
                // ModelCommandCallable is responsible for consuming any exceptions while processing
                // An exception here indicates a problem outside of the workflow.
                log.error(e.getMessage(), e);
            }
        }
   
        log.debug("Finished processing " + modelCommands.size() + " model commands: " + modelCommandsStr);
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

    public JobService getJobService() {
        return jobService;
    }

    public void setJobService(JobService jobService) {
        this.jobService = jobService;
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

    public ModelStepPostProcessor getModelStepFinishProcessor() {
        return modelStepFinishProcessor;
    }

    public void setModelStepFinishProcessor(ModelStepPostProcessor modelStepFinishProcessor) {
        this.modelStepFinishProcessor = modelStepFinishProcessor;
    }

    public ModelStepPostProcessor getModelStepOutputResultsProcessor() {
        return modelStepOutputResultsProcessor;
    }

    public void setModelStepOutputResultsProcessor(ModelStepPostProcessor modelStepOutputResultsProcessor) {
        this.modelStepOutputResultsProcessor = modelStepOutputResultsProcessor;
    }

    public int getWaitTime() {
        return waitTime;
    }

    public void setWaitTime(int waitTime) {
        this.waitTime = waitTime;
    }
    

}
