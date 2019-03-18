package com.latticeengines.workflowapi.steps.cdl;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertTrue;

import java.util.Map;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.test.StepRunner;
import org.testng.annotations.Test;

import com.latticeengines.cdl.workflow.steps.maintenance.CleanupByUploadStep;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.serviceflows.cdl.CDLOperationWorkflowConfiguration;
import com.latticeengines.domain.exposed.serviceflows.datacloud.etl.TransformationWorkflowConfiguration;
import com.latticeengines.domain.exposed.workflow.BaseStepConfiguration;
import com.latticeengines.domain.exposed.workflow.WorkflowJob;
import com.latticeengines.workflow.core.WorkflowTranslator;
import com.latticeengines.workflow.exposed.build.Choreographer;
import com.latticeengines.workflow.exposed.entitymanager.WorkflowJobEntityMgr;
import com.latticeengines.workflow.exposed.service.WorkflowService;
import com.latticeengines.workflowapi.functionalframework.WorkflowApiFunctionalTestNGBase;

public class CleanupByUploadWrapperTestNG extends WorkflowApiFunctionalTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(CleanupByUploadWrapperTestNG.class);
    @Inject
    private JobLauncher jobLauncher;

    @Inject
    private JobRepository jobRepository;

    @Inject
    private WorkflowService workflowService;

    @Inject
    private WorkflowTranslator workflowTranslator;

    @Test(groups = "deployment")
    public void test() throws Exception {
        CleanupByUploadStep cleanupByUploadStep = new CleanupByUploadStep();
        cleanupByUploadStep.setBeanName("cleanupByUploadStep");
        StepRunner runner = new StepRunner(jobLauncher, jobRepository);

        CDLOperationWorkflowConfiguration.Builder builder = new CDLOperationWorkflowConfiguration.Builder();
        builder.customer(CustomerSpace.parse("Workflow_Tenant"));
        builder.isCleanupByUpload(false, true);
        CDLOperationWorkflowConfiguration config = builder.build();

        JobParameters params = workflowService.createJobParams(config);
        log.info(params.toString());
        ExecutionContext executionContext = new ExecutionContext();

        WorkflowJob job = new WorkflowJob();
        WorkflowJobEntityMgr workflowJobEntityMgr = mock(WorkflowJobEntityMgr.class);
        when(workflowJobEntityMgr.findByWorkflowId(anyLong())).thenReturn(job);
        doNothing().when(workflowJobEntityMgr).updateWorkflowJob(any(WorkflowJob.class));
        cleanupByUploadStep.setWorkflowJobEntityMgr(workflowJobEntityMgr);
        cleanupByUploadStep.setNamespace("cdlOperationWorkflow.CleanupByUploadWrapperConfiguration");
        Choreographer choreographer = Choreographer.DEFAULT_CHOREOGRAPHER;
        JobExecution execution = runner.launchStep(workflowTranslator.step(cleanupByUploadStep, choreographer, 0, null, null),
                params, executionContext);
        while (execution.isRunning()) {
            Thread.sleep(5000);
        }

        Map<String, BaseStepConfiguration> stepConfigs = cleanupByUploadStep.getStepConfigMapInWorkflow(
                cleanupByUploadStep.getParentNamespace(), "", TransformationWorkflowConfiguration.class);
        log.info(stepConfigs.toString());
        stepConfigs.values().forEach(c -> assertTrue(c.isSkipStep()));
    }
}
