package com.latticeengines.aws.batch.impl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Regions;
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

    private static final Logger log = LoggerFactory.getLogger(BatchServiceImpl.class);

    @Value("${hadoop.leds.version}")
    private String ledsVersion;

    private AWSBatch awsBatch = null;

    @Autowired
    public BatchServiceImpl(BasicAWSCredentials awsCredentials, @Value("${aws.region}") String region) {
        log.info("Constructing AWSBatch using BasicAWSCredentials.");
        awsBatch = AWSBatchClientBuilder.standard() //
                .withCredentials(new AWSStaticCredentialsProvider(awsCredentials)) //
                .withRegion(Regions.fromName(region)) //
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
        String definitionName = request.getJobDefinition() + "-" + ledsVersion.replaceAll("[.]", "-");
        submitJobRequest.setJobDefinition(definitionName);
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
            log.info(String.format("Job Id=%s Job Name=%s, Status=%s, Log Stream Name=%s", jobDetail.getJobId(),
                    jobDetail.getJobName(), jobDetail.getStatus(),
                    jobDetail.getContainer() != null ? jobDetail.getContainer().getLogStreamName() : ""));
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

    @Override
    public String getJobStatus(String jobId) {
        DescribeJobsRequest describeJobsRequest = new DescribeJobsRequest();
        describeJobsRequest.setJobs(Arrays.asList(jobId));
        DescribeJobsResult jobResult = awsBatch.describeJobs(describeJobsRequest);
        JobDetail jobDetail = jobResult.getJobs().get(0);
        log.info(String.format("Job Id=%s Job Name=%s, Status=%s, Log Stream Name=%s", jobDetail.getJobId(),
                jobDetail.getJobName(), jobDetail.getStatus(),
                jobDetail.getContainer() != null ? jobDetail.getContainer().getLogStreamName() : ""));
        return jobDetail.getStatus();
    }
}
