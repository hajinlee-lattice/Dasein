package com.latticeengines.pls.end2end;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import java.util.List;

import javax.annotation.Nullable;

import org.apache.hadoop.conf.Configuration;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.workflow.Job;
import com.latticeengines.domain.exposed.workflow.JobStatus;
import com.latticeengines.pls.functionalframework.PlsDeploymentTestNGBase;
import com.latticeengines.pls.workflow.ScoreWorkflowSubmitter;
import com.latticeengines.workflow.exposed.WorkflowContextConstants;

public class SelfServiceModelingToBulkScoringEndToEndDeploymentTestNG extends PlsDeploymentTestNGBase {

    @Autowired
    private SelfServiceModelingEndToEndDeploymentTestNG selfServiceModeling;

    private String modelId;
    private String applicationId;

    @Autowired
    private Configuration yarnConfiguration;

    @Autowired
    private ScoreWorkflowSubmitter scoreWorkflowSubmitter;

    @BeforeClass(groups = "deployment.lp")
    public void setup() throws Exception {
        selfServiceModeling.setup();
        modelId = selfServiceModeling.prepareModel();
    }

    @Test(groups = "deployment.lp", enabled = false)
    public void testScoreTrainingData() throws Exception {
        System.out.println(String.format("%s/pls/scores/%s/training", getPLSRestAPIHostPort(), modelId));
        applicationId = selfServiceModeling.getRestTemplate().postForObject(
                String.format("%s/pls/scores/%s/training", getPLSRestAPIHostPort(), modelId), //
                null, String.class);
        assertNotNull(applicationId);
    }

    @Test(groups = "deployment.lp", dependsOnMethods = "testScoreTrainingData", timeOut = 10000, enabled = false)
    public void testJobIsListed() {
        boolean any = false;
        while (true) {
            @SuppressWarnings("unchecked")
            List<Object> raw = selfServiceModeling.getRestTemplate().getForObject(
                    String.format("%s/pls/scores/jobs/%s", modelId), List.class);
            List<Job> jobs = JsonUtils.convertList(raw, Job.class);
            any = Iterables.any(jobs, new Predicate<Job>() {

                @Override
                public boolean apply(@Nullable Job input) {
                    String jobModelId = input.getOutputs().get(WorkflowContextConstants.Outputs.MODEL_ID);
                    return input.getJobType() != null && input.getJobType().equals("scoreWorkflow")
                            && modelId.equals(jobModelId);
                }
            });

            if (any) {
                break;
            }
            sleep(500);
        }

        assertTrue(any);
    }

    @Test(groups = "deployment.lp", dependsOnMethods = "testJobIsListed", timeOut = 480000, enabled = false)
    public void poll() {
        JobStatus terminal;
        while (true) {
            Job job = selfServiceModeling.getRestTemplate().getForObject(
                    String.format("%s/pls/jobs/yarnapps/%s", getPLSRestAPIHostPort(), applicationId), //
                    null, String.class);
            assertNotNull(job);
            if (Job.TERMINAL_JOB_STATUS.contains(job.getJobStatus())) {
                terminal = job.getJobStatus();
                break;
            }
        }

        assertEquals(terminal, JobStatus.COMPLETED);
    }

    private void sleep(long msec) {
        try {
            Thread.sleep(msec);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

}
