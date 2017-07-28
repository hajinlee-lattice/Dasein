package com.latticeengines.aws.batch.impl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.batch.AWSBatch;
import com.amazonaws.services.batch.AWSBatchClientBuilder;
import com.amazonaws.services.batch.model.ContainerOverrides;
import com.amazonaws.services.batch.model.DescribeJobsRequest;
import com.amazonaws.services.batch.model.DescribeJobsResult;
import com.amazonaws.services.batch.model.JobDetail;
import com.amazonaws.services.batch.model.KeyValuePair;
import com.amazonaws.services.batch.model.SubmitJobRequest;
import com.amazonaws.services.batch.model.SubmitJobResult;
import com.latticeengines.aws.batch.BatchService;
import com.latticeengines.aws.batch.JobRequest;

@Component("batchService")
public class BatchServiceImpl implements BatchService {

    private static final Log log = LogFactory.getLog(BatchServiceImpl.class);

    private AWSBatch awsBatch = null;

    @Autowired
    public BatchServiceImpl(BasicAWSCredentials awsCredentials) {
        log.info("Constructing AWSBatch using BasicAWSCredentials.");
        awsBatch = AWSBatchClientBuilder.standard().withCredentials(new AWSStaticCredentialsProvider(awsCredentials))
                .build();
    }

    @Override
    public String submitJob(JobRequest request) {
        SubmitJobResult result = awsBatch.submitJob(toJobRequest(request));
        return result.getJobId();
    }

    private SubmitJobRequest toJobRequest(JobRequest request) {
        SubmitJobRequest submitJobRequest = new SubmitJobRequest();
        submitJobRequest.setJobName(request.getJobName());
        submitJobRequest.setJobDefinition(request.getJobDefinition());
        submitJobRequest.setJobQueue(request.getJobQueue());
        submitJobRequest.setParameters(request.getParameters());

        ContainerOverrides overrides = new ContainerOverrides();
        List<KeyValuePair> envs = new ArrayList<>();
        request.getEnvs().forEach((name, value) -> {
            envs.add(new KeyValuePair().withName(name).withValue(value));
        });
        overrides.setEnvironment(envs);
        overrides.setMemory(request.getMemory());
        overrides.setVcpus(request.getCpus());

        submitJobRequest.setContainerOverrides(overrides);
        return submitJobRequest;
    }

    @Override
    public boolean waitForCompletion(String jobId, long maxWaitTime) {
        long startTime = System.currentTimeMillis();
        DescribeJobsRequest describeJobsRequest = new DescribeJobsRequest();
        describeJobsRequest.setJobs(Arrays.asList(jobId));
        String status = null;
        do {
            DescribeJobsResult jobResult = awsBatch.describeJobs(describeJobsRequest);
            JobDetail jobDetail = jobResult.getJobs().get(0);
            status = jobDetail.getStatus();
            log.info("Job name=" + jobDetail.getJobName() + " Job id=" + jobId + " Status=" + jobDetail.getStatus());
            if (status.equalsIgnoreCase("SUCCEEDED")) {
                return true;
            }
            if (status.equalsIgnoreCase("FAILED")) {
                log.info("Job name=" + jobDetail.getJobName() + " Job id=" + jobId + " Status=" + jobDetail.getStatus()
                        + " Status reason=" + jobDetail.getStatusReason());
                return false;
            }
            try {
                Thread.sleep(10_000L);
            } catch (Exception ex) {
                log.warn("Wait was interrupted, message=" + ex.getMessage());
            }
        } while ((System.currentTimeMillis() - startTime) < maxWaitTime);

        throw new RuntimeException("Job Id:" + jobId + " timeout within (ms)=" + maxWaitTime);
    }
}
