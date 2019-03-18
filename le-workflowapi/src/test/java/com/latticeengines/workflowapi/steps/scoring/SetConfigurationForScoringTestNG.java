package com.latticeengines.workflowapi.steps.scoring;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

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

import com.google.common.collect.ImmutableMap;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.serviceflows.cdl.CrossSellImportMatchAndModelWorkflowConfiguration;
import com.latticeengines.domain.exposed.workflow.WorkflowContextConstants;
import com.latticeengines.domain.exposed.workflow.WorkflowJob;
import com.latticeengines.scoring.workflow.steps.SetConfigurationForScoring;
import com.latticeengines.workflow.core.WorkflowTranslator;
import com.latticeengines.workflow.exposed.build.Choreographer;
import com.latticeengines.workflow.exposed.entitymanager.WorkflowJobEntityMgr;
import com.latticeengines.workflow.exposed.service.WorkflowService;
import com.latticeengines.workflowapi.functionalframework.WorkflowApiFunctionalTestNGBase;

public class SetConfigurationForScoringTestNG extends WorkflowApiFunctionalTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(SetConfigurationForScoringTestNG.class);

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
        SetConfigurationForScoring setConfigurationForScoring = new SetConfigurationForScoring();
        setConfigurationForScoring.setBeanName("setConfigurationForScoring");
        StepRunner runner = new StepRunner(jobLauncher, jobRepository);

        CrossSellImportMatchAndModelWorkflowConfiguration.Builder builder = new CrossSellImportMatchAndModelWorkflowConfiguration.Builder();
        builder.customer(CustomerSpace.parse("Workflow_Tenant"));
        builder.modelingServiceHdfsBaseDir("abc");
        builder.microServiceHostPort("123");
        builder.inputProperties(ImmutableMap.of(WorkflowContextConstants.Inputs.SOURCE_DISPLAY_NAME, "abc.csv"));
        CrossSellImportMatchAndModelWorkflowConfiguration config = builder.build();

        JobParameters params = workflowService.createJobParams(config);
        log.info(params.toString());
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
        setConfigurationForScoring
                .setNamespace("crossSellImportMatchAndModelWorkflow.SetConfigurationForScoringConfiguration");
        Choreographer choreographer = Choreographer.DEFAULT_CHOREOGRAPHER;
        JobExecution execution = runner.launchStep(
                workflowTranslator.step(setConfigurationForScoring, choreographer, 0, null, null), params, executionContext);
        while (execution.isRunning()) {
            Thread.sleep(5000);
        }

        assertNotNull(setConfigurationForScoring.getStringValueFromContext("EXPORT_SCORE_TRAINING_FILE_OUTPUT_PATH"));
        assertTrue(setConfigurationForScoring.getStringValueFromContext("EXPORT_SCORE_TRAINING_FILE_OUTPUT_PATH")
                .contains("score_event_table_output"));
        assertTrue(setConfigurationForScoring.getStringValueFromContext("EXPORT_SCORE_TRAINING_FILE_OUTPUT_PATH")
                .contains(t2.getName()));
        assertEquals(setConfigurationForScoring.getObjectFromContext("EVENT_TABLE", Table.class).toString(),
                t2.toString());
    }
}
