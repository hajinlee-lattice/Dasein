package com.latticeengines.pls.end2end;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import java.util.Arrays;
import java.util.List;

import javax.annotation.Nullable;

import org.apache.hadoop.conf.Configuration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.ByteArrayHttpMessageConverter;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.workflow.Job;
import com.latticeengines.domain.exposed.workflow.JobStatus;
import com.latticeengines.pls.functionalframework.PlsDeploymentTestNGBaseDeprecated;
import com.latticeengines.pls.workflow.ScoreWorkflowSubmitter;
import com.latticeengines.workflow.exposed.WorkflowContextConstants;

public class SelfServiceModelingToBulkScoringEndToEndDeploymentTestNG extends PlsDeploymentTestNGBaseDeprecated {

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

    @Test(groups = "deployment.lp")
    public void testScoreTrainingData() throws Exception {
        System.out.println(String.format("%s/pls/scores/%s/training", getPLSRestAPIHostPort(), modelId));
        applicationId = selfServiceModeling.getRestTemplate().postForObject(
                String.format("%s/pls/scores/%s/training", getPLSRestAPIHostPort(), modelId), //
                null, String.class);
        System.out.println(String.format("Score training data applicationId = %s", applicationId));
        assertNotNull(applicationId);
    }

    @Test(groups = "deployment.lp", dependsOnMethods = "testScoreTrainingData", timeOut = 60000)
    public void testJobIsListed() {
        boolean any = false;
        while (true) {
            @SuppressWarnings("unchecked")
            List<Object> raw = selfServiceModeling.getRestTemplate().getForObject(
                    String.format("%s/pls/scores/jobs/%s", getPLSRestAPIHostPort(), modelId), List.class);
            List<Job> jobs = JsonUtils.convertList(raw, Job.class);
            any = Iterables.any(jobs, new Predicate<Job>() {

                @Override
                public boolean apply(@Nullable Job job) {
                    String jobModelId = job.getInputs().get(WorkflowContextConstants.Inputs.MODEL_ID);
                    return job.getJobType() != null && job.getJobType().equals("scoreWorkflow")
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

    @Test(groups = "deployment.lp", dependsOnMethods = "testJobIsListed", timeOut = 1800000)
    public void poll() {
        JobStatus terminal;
        while (true) {
            Job job = selfServiceModeling.getRestTemplate().getForObject(
                    String.format("%s/pls/jobs/yarnapps/%s", getPLSRestAPIHostPort(), applicationId), Job.class);
            assertNotNull(job);
            if (Job.TERMINAL_JOB_STATUS.contains(job.getJobStatus())) {
                terminal = job.getJobStatus();
                break;
            }
            sleep(1000);
        }

        assertEquals(terminal, JobStatus.COMPLETED);
    }

    @Test(groups = "deployment.lp", dependsOnMethods = "poll")
    public void downloadCsv() {
        selfServiceModeling.getRestTemplate().getMessageConverters().add(new ByteArrayHttpMessageConverter());
        HttpHeaders headers = new HttpHeaders();
        headers.setAccept(Arrays.asList(MediaType.ALL));
        HttpEntity<String> entity = new HttpEntity<>(headers);
        ResponseEntity<byte[]> response = selfServiceModeling.getRestTemplate().exchange(
                String.format("%s/pls/scores/jobs/%s/results", getPLSRestAPIHostPort(), applicationId), HttpMethod.GET,
                entity, byte[].class);
        assertEquals(response.getStatusCode(), HttpStatus.OK);
        String results = new String(response.getBody());
        assertTrue(results.length() > 0);
    }

    private void sleep(long msec) {
        try {
            Thread.sleep(msec);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

}
