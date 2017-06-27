package com.latticeengines.proxy.exposed.workflowapi;

import java.util.List;

import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.api.AppSubmission;
import com.latticeengines.domain.exposed.workflow.Job;
import com.latticeengines.domain.exposed.workflow.WorkflowConfiguration;
import com.latticeengines.domain.exposed.workflow.WorkflowExecutionId;
import com.latticeengines.domain.exposed.workflow.WorkflowStatus;
import com.latticeengines.network.exposed.workflowapi.WorkflowInterface;
import com.latticeengines.proxy.exposed.MicroserviceRestApiProxy;

@Component
public class WorkflowProxy extends MicroserviceRestApiProxy implements WorkflowInterface {

    public WorkflowProxy() {
        super("workflowapi/workflows");
    }

    @Override
    public AppSubmission submitWorkflowExecution(WorkflowConfiguration config) {
        String url = constructUrl("/");
        return post("submitWorkflowExecution", url, config, AppSubmission.class);
    }

    @Override
    public AppSubmission restartWorkflowExecution(Long workflowId) {
        String url = constructUrl("/job/{workflowId}/restart", workflowId);
        return post("restartWorkflow", url, null, AppSubmission.class);
    }

    @Override
    public WorkflowExecutionId getWorkflowId(String applicationId) {
        String url = constructUrl("/yarnapps/id/{applicationId}", applicationId);
        return get("getWorkflowId", url, WorkflowExecutionId.class);
    }

    @Override
    @Deprecated
    public WorkflowStatus getWorkflowStatus(String workflowId) {
        String url = constructUrl("/status/{workflowId}", workflowId);
        return get("getWorkflowStatus", url, WorkflowStatus.class);
    }

    @Override
    public Job getWorkflowJobFromApplicationId(String applicationId) {
        String url = constructUrl("/yarnapps/jobs/{applicationId}", applicationId);
        return get("getWorkflowJobFromApplicationId", url, Job.class);
    }

    @Override
    public Job getWorkflowExecution(String workflowId) {
        String url = constructUrl("/job/{workflowId}", workflowId);
        return get("getWorkflowExecution", url, Job.class);
    }

    @Override
    public List<Job> getWorkflowExecutionsForTenant(long tenantPid) {
        String url = constructUrl("/jobs/{tenantPid}", tenantPid);
        return JsonUtils.convertList(get("getWorkflowExecutionsForTenant", url, List.class), Job.class);
    }

    @Override
    public List<Job> getWorkflowExecutionsForTenant(long tenantPid, String type) {
        String url = constructUrl("/jobs/{tenantPid}/find?type={type}", tenantPid, type);
        return JsonUtils.convertList(get("getWorkflowExecutionsForTenant", url, List.class), Job.class);
    }

    @Override
    public void stopWorkflow(String workflowId) {
        String url = constructUrl("/job/{workflowId}/stop", workflowId);
        post("stopWorkflow", url, null, Void.class);
    }

}
