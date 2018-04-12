package com.latticeengines.apps.core.service.impl;

import javax.inject.Inject;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.apps.core.service.WorkflowJobService;
import com.latticeengines.domain.exposed.api.AppSubmission;
import com.latticeengines.domain.exposed.workflow.Job;
import com.latticeengines.domain.exposed.workflow.JobStatus;
import com.latticeengines.domain.exposed.workflow.WorkflowConfiguration;
import com.latticeengines.proxy.exposed.workflowapi.WorkflowProxy;

@Component("workflowJobService")
public class WorkflowJobServiceImpl implements WorkflowJobService {

    private static final Logger log = LoggerFactory.getLogger(WorkflowJobService.class);

    private final WorkflowProxy workflowProxy;

    @Inject
    public WorkflowJobServiceImpl(WorkflowProxy workflowProxy) {
        this.workflowProxy = workflowProxy;
    }

    @Override
    public ApplicationId restart(Long jobId, String customerSpace) {
        AppSubmission submission = workflowProxy.restartWorkflowExecution(String.valueOf(jobId), customerSpace);
        String applicationId = submission.getApplicationIds().get(0);

        log.info(String.format("Resubmitted workflow with application id %s", applicationId));
        return ConverterUtils.toApplicationId(applicationId);
    }

    @Override
    public ApplicationId submit(WorkflowConfiguration configuration) {
        return submit(configuration, null);
    }

    @Override
    public ApplicationId submit(WorkflowConfiguration configuration, Long workflowPid) {
        AppSubmission submission;

        if (workflowPid != null) {
            submission = workflowProxy.submitWorkflow(configuration, workflowPid);
        } else {
            submission = workflowProxy.submitWorkflowExecution(configuration);
        }

        String applicationId = submission.getApplicationIds().get(0);
        log.info(String.format("Submitted %s with application id %s", configuration.getWorkflowName(), applicationId));
        return ConverterUtils.toApplicationId(applicationId);
    }

    @Override
    public JobStatus getJobStatusFromApplicationId(String appId) {
        Job job = workflowProxy.getWorkflowJobFromApplicationId(appId);
        return job.getJobStatus();
    }

}
