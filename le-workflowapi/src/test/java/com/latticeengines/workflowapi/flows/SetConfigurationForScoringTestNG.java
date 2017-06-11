package com.latticeengines.workflowapi.flows;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import org.apache.commons.io.IOUtils;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.test.StepRunner;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.workflow.WorkflowJob;
import com.latticeengines.domain.exposed.serviceflows.leadprioritization.ImportMatchAndModelWorkflowConfiguration;
import com.latticeengines.leadprioritization.workflow.steps.SetConfigurationForScoring;
import com.latticeengines.domain.exposed.serviceflows.core.steps.ExportStepConfiguration;
import com.latticeengines.workflow.core.WorkflowTranslator;
import com.latticeengines.workflow.exposed.entitymanager.WorkflowJobEntityMgr;
import com.latticeengines.workflow.exposed.service.WorkflowService;
import com.latticeengines.workflowapi.functionalframework.WorkflowApiFunctionalTestNGBase;

public class SetConfigurationForScoringTestNG extends WorkflowApiFunctionalTestNGBase {

    @Autowired
    private JobLauncher jobLauncher;

    @Autowired
    private JobRepository jobRepository;

    @Autowired
    private SetConfigurationForScoring setConfigurationForScoring;

    @Autowired
    private WorkflowService workflowService;

    @Autowired
    private WorkflowTranslator workflowTranslator;

    @Test(groups = "functional")
    public void test() throws Exception {
        StepRunner runner = new StepRunner(jobLauncher, jobRepository);
        String jsonStr = IOUtils.toString(ClassLoader
                .getSystemResourceAsStream("com/latticeengines/workflowapi/flows/leadprioritization/workflow.conf"));
        ImportMatchAndModelWorkflowConfiguration config = JsonUtils.deserialize(jsonStr,
                ImportMatchAndModelWorkflowConfiguration.class);
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

        runner.launchStep(workflowTranslator.step(setConfigurationForScoring), params, executionContext);
        Thread.sleep(10000);
        assertTrue(setConfigurationForScoring
                .getObjectFromContext(ExportStepConfiguration.class.getName(), ExportStepConfiguration.class)
                .getUsingDisplayName());

        assertEquals(setConfigurationForScoring.getStringValueFromContext("EXPORT_INPUT_PATH"), "");
        assertTrue(setConfigurationForScoring.getStringValueFromContext("EXPORT_OUTPUT_PATH")
                .contains("score_event_table_output"));
    }
}
