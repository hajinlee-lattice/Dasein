package com.latticeengines.workflowapi.flows;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import org.springframework.batch.core.JobExecution;
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
import com.latticeengines.domain.exposed.serviceflows.leadprioritization.ImportMatchAndModelWorkflowConfiguration;
import com.latticeengines.domain.exposed.workflow.WorkflowContextConstants;
import com.latticeengines.domain.exposed.workflow.WorkflowJob;
import com.latticeengines.scoring.workflow.steps.SetConfigurationForScoring;
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
        System.out.print(params);
        ExecutionContext executionContext = new ExecutionContext();

        Table t1 = new Table();
        t1.setName("table1");

        Table t2 = new Table();
        t2.setName("table2");

        executionContext.putString("MATCH_RESULT_TABLE", JsonUtils.serialize(t1));
        executionContext.putString("EVENT_TABLE", JsonUtils.serialize(t2));
        executionContext.putString("SCORING_MODEL_ID", "ms__default_id_");
        executionContext.putString("SCORING_MODEL_TYPE", "pmml");

        WorkflowJob job = new WorkflowJob();
        WorkflowJobEntityMgr workflowJobEntityMgr = mock(WorkflowJobEntityMgr.class);
        when(workflowJobEntityMgr.findByWorkflowId(anyLong())).thenReturn(job);
        doNothing().when(workflowJobEntityMgr).updateWorkflowJob(any(WorkflowJob.class));
        setConfigurationForScoring.setWorkflowJobEntityMgr(workflowJobEntityMgr);
        setConfigurationForScoring.setNamespace("importMatchAndModelWorkflow.SetConfigurationForScoringConfiguration");
        Choreographer choreographer = Choreographer.DEFAULT_CHOREOGRAPHER;
        JobExecution execution = runner.launchStep(
                workflowTranslator.step(setConfigurationForScoring, choreographer, 0), params, executionContext);
        while (execution.isRunning()) {
            Thread.sleep(5000);
        }

        assertNull(setConfigurationForScoring.getStringValueFromContext("EXPORT_INPUT_PATH"));
        assertTrue(setConfigurationForScoring.getStringValueFromContext("EXPORT_OUTPUT_PATH")
                .contains("score_event_table_output"));
        assertTrue(setConfigurationForScoring.getStringValueFromContext("EXPORT_OUTPUT_PATH").contains(t2.getName()));
        assertEquals(setConfigurationForScoring.getObjectFromContext("EVENT_TABLE", Table.class).toString(),
                t1.toString());
    }
}
