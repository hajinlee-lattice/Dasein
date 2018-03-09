package com.latticeengines.workflowapi.flows;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import javax.inject.Inject;

import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.test.StepRunner;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.util.ReflectionTestUtils;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableMap;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.serviceflows.core.steps.ExportStepConfiguration;
import com.latticeengines.domain.exposed.serviceflows.leadprioritization.ImportMatchAndModelWorkflowConfiguration;
import com.latticeengines.domain.exposed.workflow.WorkflowContextConstants;
import com.latticeengines.domain.exposed.workflow.WorkflowJob;
import com.latticeengines.scoring.workflow.RTSBulkScoreWorkflow;
import com.latticeengines.scoring.workflow.steps.SetConfigurationForScoring;
import com.latticeengines.serviceflows.workflow.match.MatchDataCloudWorkflow;
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

    @Inject
    private RTSBulkScoreWorkflow rtsBulkScoreWorkflow;

    @Inject
    private MatchDataCloudWorkflow matchDataCloudWorkflow;

    @Test(groups = "workflow")
    public void test() throws Exception {
        SetConfigurationForScoring setConfigurationForScoring = new SetConfigurationForScoring();
        setConfigurationForScoring.setBeanName("setConfigurationForScoring");
        ReflectionTestUtils.setField(setConfigurationForScoring, "rtsBulkScoreWorkflow", rtsBulkScoreWorkflow);
        ReflectionTestUtils.setField(setConfigurationForScoring, "matchDataCloudWorkflow", matchDataCloudWorkflow);
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

        Table t = new Table();
        t.setName("table");
        executionContext.putString("MATCH_RESULT_TABLE", JsonUtils.serialize(t));
        executionContext.putString("EVENT_TABLE", JsonUtils.serialize(t));

        WorkflowJob job = new WorkflowJob();
        WorkflowJobEntityMgr workflowJobEntityMgr = mock(WorkflowJobEntityMgr.class);
        when(workflowJobEntityMgr.findByWorkflowId(anyLong())).thenReturn(job);
        doNothing().when(workflowJobEntityMgr).updateWorkflowJob(any(WorkflowJob.class));
        setConfigurationForScoring.setWorkflowJobEntityMgr(workflowJobEntityMgr);
        setConfigurationForScoring.setNamespace("importMatchAndModelWorkflow.SetConfigurationForScoringConfiguration");
        Choreographer choreographer = Choreographer.DEFAULT_CHOREOGRAPHER;
        runner.launchStep(workflowTranslator.step(setConfigurationForScoring, choreographer, 0), params,
                executionContext);
        Thread.sleep(10000);

        String parentNamespace = setConfigurationForScoring.getNamespace().substring(0,
                setConfigurationForScoring.getNamespace().lastIndexOf('.'));

        String exportStepStepNamespace = String.join(".", parentNamespace, rtsBulkScoreWorkflow.name(),
                ExportStepConfiguration.class.getSimpleName());

        assertTrue(setConfigurationForScoring
                .getObjectFromContext(exportStepStepNamespace, ExportStepConfiguration.class).getUsingDisplayName());

        assertEquals(setConfigurationForScoring.getStringValueFromContext("EXPORT_INPUT_PATH"), "");
        assertTrue(setConfigurationForScoring.getStringValueFromContext("EXPORT_OUTPUT_PATH")
                .contains("score_event_table_output"));
    }
}
