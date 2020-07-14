package com.latticeengines.testframework.service.impl;

import java.util.List;
import java.util.concurrent.TimeoutException;

import javax.annotation.Resource;
import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import com.latticeengines.domain.exposed.workflow.Job;
import com.latticeengines.domain.exposed.workflow.JobStatus;
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
}
