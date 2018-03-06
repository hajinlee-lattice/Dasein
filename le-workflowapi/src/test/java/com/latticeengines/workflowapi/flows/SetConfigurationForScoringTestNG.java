package com.latticeengines.workflowapi.flows;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.test.StepRunner;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableMap;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.serviceflows.core.steps.ExportStepConfiguration;
import com.latticeengines.domain.exposed.serviceflows.leadprioritization.ImportMatchAndModelWorkflowConfiguration;
import com.latticeengines.domain.exposed.workflow.WorkflowContextConstants;
import com.latticeengines.domain.exposed.workflow.WorkflowJob;
import com.latticeengines.modeling.workflow.steps.SetConfigurationForScoring;
import com.latticeengines.workflow.core.WorkflowTranslator;
import com.latticeengines.workflow.exposed.build.Choreographer;
import com.latticeengines.workflow.exposed.entitymanager.WorkflowJobEntityMgr;
import com.latticeengines.workflow.exposed.service.WorkflowService;
import com.latticeengines.workflowapi.functionalframework.WorkflowApiFunctionalTestNGBase;

public class SetConfigurationForScoringTestNG extends WorkflowApiFunctionalTestNGBase {

    @Autowired
    private JobLauncher jobLauncher;

    @Autowired
    private JobRepository jobRepository;

    @Autowired
    private WorkflowService workflowService;

    @Autowired
    private WorkflowTranslator workflowTranslator;

    @Test(groups = "workflow")
    public void test() throws Exception {
        SetConfigurationForScoring setConfigurationForScoring = new SetConfigurationForScoring();
        setConfigurationForScoring.setBeanName("setConfigurationForScoring");
        StepRunner runner = new StepRunner(jobLauncher, jobRepository);

        ImportMatchAndModelWorkflowConfiguration.Builder builder = new ImportMatchAndModelWorkflowConfiguration.Builder();
        builder.customer(CustomerSpace.parse("Workflow_Tenant"));
        builder.modelingServiceHdfsBaseDir("abc");
        builder.microServiceHostPort("123");
        builder.inputProperties(ImmutableMap.of(WorkflowContextConstants.Inputs.SOURCE_DISPLAY_NAME, "abc.csv"));
        ImportMatchAndModelWorkflowConfiguration config = builder.build();

        JobParameters params = workflowService.createJobParams(config);
        ExecutionContext executionContext = new ExecutionContext();

        Table t = new Table();
        t.setName("table");
        executionContext.putString("MATCH_RESULT_TABLE", JsonUtils.serialize(t));
        executionContext.putString("EVENT_TABLE", JsonUtils.serialize(t));

        WorkflowJob job = new WorkflowJob();
        WorkflowJobEntityMgr workflowJobEntityMgr = mock(WorkflowJobEntityMgr.class);
        when(workflowJobEntityMgr.findByWorkflowId(anyLong())).thenReturn(job);
        doNothing().when(workflowJobEntityMgr).updateWorkflowJob(any(WorkflowJob.class));
        setConfigurationForScoring.setWorkflowJobEntityMgr(workflowJobEntityMgr);

        Choreographer choreographer = Choreographer.DEFAULT_CHOREOGRAPHER;
        runner.launchStep(workflowTranslator.step(setConfigurationForScoring, choreographer, 0), params,
                executionContext);
        Thread.sleep(10000);
        assertTrue(setConfigurationForScoring
                .getObjectFromContext(ExportStepConfiguration.class.getName(), ExportStepConfiguration.class)
                .getUsingDisplayName());

        assertEquals(setConfigurationForScoring.getStringValueFromContext("EXPORT_INPUT_PATH"), "");
        assertTrue(setConfigurationForScoring.getStringValueFromContext("EXPORT_OUTPUT_PATH")
                .contains("score_event_table_output"));
    }
}
