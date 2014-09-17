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
import com.latticeengines.dataplatform.service.dlorchestration.DLOrchestrationService;
import com.latticeengines.dataplatform.service.dlorchestration.ModelCommandLogService;
import com.latticeengines.dataplatform.service.dlorchestration.ModelStepProcessor;
import com.latticeengines.dataplatform.service.dlorchestration.ModelStepYarnProcessor;
import com.latticeengines.dataplatform.service.modeling.ModelingJobService;
import com.latticeengines.domain.exposed.dataplatform.dlorchestration.ModelCommand;

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

    private int waitTime = 180;

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
            futures.add(dlOrchestrationJobTaskExecutor.submit(new ModelCommandCallable(modelCommand,
                    modelingJobService, modelCommandEntityMgr, modelCommandStateEntityMgr, modelStepYarnProcessor,
                    modelCommandLogService, modelCommandResultEntityMgr, modelStepFinishProcessor,
                    modelStepOutputResultsProcessor, modelStepRetrieveMetadataProcessor)));
        }
        for (Future<Long> future : futures) {
            try {
                future.get(waitTime, TimeUnit.SECONDS);
            } catch (Exception e) {
                // ModelCommandCallable is responsible for consuming any
                // exceptions while processing
                // An exception here indicates a problem outside of the
                // workflow.
                log.error(e.getMessage(), e);
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

}
