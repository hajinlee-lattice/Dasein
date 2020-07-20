package com.latticeengines.testframework.service.impl;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeoutException;

import javax.annotation.Resource;
import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.stereotype.Service;
import org.testng.Assert;

import com.latticeengines.common.exposed.util.RetryUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.ProcessAnalyzeRequest;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.util.ApplicationIdUtils;
import com.latticeengines.domain.exposed.workflow.Job;
import com.latticeengines.domain.exposed.workflow.JobStatus;
import com.latticeengines.proxy.exposed.cdl.CDLProxy;
import com.latticeengines.proxy.exposed.workflowapi.WorkflowProxy;
import com.latticeengines.testframework.exposed.proxy.pls.PlsJobProxy;
import com.latticeengines.testframework.exposed.service.TestJobService;
import com.latticeengines.testframework.exposed.utils.TestControlUtils;

@Service("testJobServiceImpl")
public class TestJobServiceImpl implements TestJobService {
    private static final Logger log = LoggerFactory.getLogger(TestJobServiceImpl.class);
    private static final String PA_JOB_TYPE = "processAnalyzeWorkflow";

    @Resource(name = "deploymentTestBed")
    private GlobalAuthDeploymentTestBed deploymentTestBed;

    @Inject
    private PlsJobProxy plsJobProxy;

    @Inject
    private CDLProxy cdlProxy;

    @Inject
    private WorkflowProxy workflowProxy;

    @Override
    public void waitForProcessAnalyzeAllActionsDone(int maxWaitInMinutes) throws TimeoutException {
        TestControlUtils.defaultWait(this::isProcessAnalyzeAllActionsDone, true, maxWaitInMinutes);
    }

    public boolean isProcessAnalyzeAllActionsDone() {
        deploymentTestBed.attachProtectedProxy(plsJobProxy);
        List<Job> jobs = plsJobProxy.getAllJobs();
        int size = jobs.size();
        boolean isReady = false;
        Job lastJob = jobs.get(size - 1);
        Job lastButOneJob = size > 1 ? jobs.get(size - 2) : null;
        if (null != lastButOneJob && PA_JOB_TYPE.equals(lastButOneJob.getJobType())) {
            JobStatus lastButOneJobStatus = lastButOneJob.getJobStatus();
            if (!(JobStatus.COMPLETED.equals(lastButOneJobStatus) || JobStatus.FAILED.equals(lastButOneJobStatus))) {
                return false;
            }
        }
        if (StringUtils.isEmpty(lastJob.getApplicationId()) && PA_JOB_TYPE.equals(lastJob.getJobType())) {
            List<Job> subJobs = lastJob.getSubJobs();
            isReady = true;
            if (null != subJobs) {
                for (int j = subJobs.size() - 1; j >= 0; --j) {
                    Job subJob = subJobs.get(j);
                    JobStatus status = subJob.getJobStatus();
                    isReady = !status.equals(JobStatus.RUNNING) && !status.equals(JobStatus.PENDING);
                    if (!isReady) {
                        break;
                    }
                }
            }
        }
        return isReady;
    }

    @Override
    public JobStatus waitForWorkflowStatus(Tenant tenant, String applicationId, boolean running) {
        String trueAppId = waitForTrueApplicationId(tenant, applicationId);
        if (!trueAppId.equals(applicationId)) {
            log.info("Convert fake app id " + applicationId + " to true app id " + trueAppId);
        }
        int retryOnException = 4;
        Job job;
        while (true) {
            try {
                job = workflowProxy.getWorkflowJobFromApplicationId(trueAppId,
                        CustomerSpace.parse(tenant.getId()).toString());
            } catch (Exception e) {
                log.error(String.format("Workflow job exception: %s", e.getMessage()), e);

                job = null;
                if (--retryOnException == 0)
                    throw new RuntimeException(e);
            }

            if ((job != null) && ((running && job.isRunning()) || (!running && !job.isRunning()))) {
                if (job.getJobStatus() == JobStatus.FAILED || job.getJobStatus() == JobStatus.PENDING_RETRY) {
                    log.error(applicationId + " Failed with ErrorCode " + job.getErrorCode() + ". \n"
                            + job.getErrorMsg());
                }
                return job.getJobStatus();
            }
            try {
                Thread.sleep(30000L);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    protected String waitForTrueApplicationId(Tenant tenant, String applicationId) {
        String customerSpace = CustomerSpace.parse(tenant.getId()).toString();
        if (StringUtils.isBlank(applicationId)) {
            throw new IllegalArgumentException("Must provide a valid fake application id");
        }
        if (!ApplicationIdUtils.isFakeApplicationId(applicationId)) {
            return applicationId;
        }
        RetryTemplate retry = RetryUtils.getExponentialBackoffRetryTemplate( //
                100, 1000, 2, 3000, //
                false, Collections.emptyMap());
        try {
            return retry.execute(ctx -> {
                if (ctx.getLastThrowable() != null) {
                    log.error("Failed to retrieve Job using application id " + applicationId, ctx.getLastThrowable());
                }
                Job job = workflowProxy.getWorkflowJobFromApplicationId(applicationId, customerSpace);
                String newId = job.getApplicationId();
                if (!ApplicationIdUtils.isFakeApplicationId(newId)) {
                    return newId;
                } else {
                    throw new IllegalStateException("Still showing fake id " + newId);
                }
            });
        } catch (Exception e) {
            throw new RuntimeException(
                    "Failed to retrieve the true application id from fake id [" + applicationId + "]");
        }
    }

    @Override
    public void processAnalyzeRunNow(Tenant tenant) {
        processAnalyze(tenant, true, null);
    }

    @Override
    public void processAnalyze(Tenant tenant, boolean runNow, ProcessAnalyzeRequest processAnalyzeRequest) {
        log.info("Start processing and analyzing on tenant {}", tenant);
        ApplicationId applicationId = cdlProxy.scheduleProcessAnalyze(CustomerSpace.parse(tenant.getId()).toString(),
                runNow, processAnalyzeRequest);
        log.info("Got applicationId={}", applicationId);

        log.info("Waiting job...");
        JobStatus jobStatus = waitForWorkflowStatus(tenant, applicationId.toString(), false);
        Assert.assertEquals(jobStatus, JobStatus.COMPLETED,
                String.format("The PA job %s cannot be completed", applicationId));

        log.info("The PA job {} is completed", applicationId);
    }

    @Override
    public Job getWorkflowJobFromApplicationId(Tenant tenant, String applicationId) {
        String trueAppId = waitForTrueApplicationId(tenant, applicationId);
        if (!trueAppId.equals(applicationId)) {
            log.info("Convert fake app id " + applicationId + " to true app id " + trueAppId);
        }
        return workflowProxy.getWorkflowJobFromApplicationId(trueAppId, CustomerSpace.parse(tenant.getId()).toString());
    }
}
