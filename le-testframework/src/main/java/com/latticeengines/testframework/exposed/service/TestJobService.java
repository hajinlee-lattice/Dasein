package com.latticeengines.testframework.exposed.service;

import java.util.concurrent.TimeoutException;

import com.latticeengines.domain.exposed.cdl.ProcessAnalyzeRequest;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.workflow.Job;
import com.latticeengines.domain.exposed.workflow.JobStatus;

public interface TestJobService {
    void waitForProcessAnalyzeAllActionsDone(int maxWaitInMinutes) throws TimeoutException;

    JobStatus waitForWorkflowStatus(Tenant tenant, String applicationId, boolean running);

    void processAnalyzeRunNow(Tenant tenant);

    void processAnalyze(Tenant tenant, boolean runNow, ProcessAnalyzeRequest processAnalyzeRequest);

    Job getWorkflowJobFromApplicationId(Tenant tenant, String applicationId);
}
