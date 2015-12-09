package com.latticeengines.proxy.exposed.workflowapi;
import java.util.List;

import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.api.AppSubmission;
import com.latticeengines.domain.exposed.workflow.Job;
import com.latticeengines.domain.exposed.workflow.WorkflowConfiguration;
import com.latticeengines.domain.exposed.workflow.WorkflowStatus;
import com.latticeengines.network.exposed.workflowapi.WorkflowInterface;
import com.latticeengines.proxy.exposed.BaseRestApiProxy;

@Component
public class WorkflowProxy extends BaseRestApiProxy implements WorkflowInterface {

    public WorkflowProxy() {
        super("workflowapi/workflows");
    }

    @Override
    public AppSubmission submitWorkflowExecution(WorkflowConfiguration config) {
        String url = constructUrl("/");
        return post("submitWorkflowExecution", url, config, AppSubmission.class);
    }

    @Override
    public String getWorkflowId(String applicationId) {
        String url = constructUrl("/yarnapps/id/{applicationId}", applicationId);
        return get("getWorkflowId", url, String.class);
    }

    @Override
    public WorkflowStatus getWorkflowStatus(String workflowId) {
        String url = constructUrl("/status/{workflowId}", workflowId);
        return get("getWorkflowStatus", url, WorkflowStatus.class);
    }

    @Override
    public WorkflowStatus getWorkflowStatusFromApplicationId(String applicationId) {
        String url = constructUrl("yarnapps/status/" + applicationId);
        return get("getWorkflowStatusFromApplicationId", url, WorkflowStatus.class);
    }

    @Override
    public Job getWorkflowExecution(String workflowId) {
        String url = constructUrl("job/" + workflowId);
        return get("getWorkflowExecution", url, Job.class);
    }

    @Override
    @SuppressWarnings("unchecked")
    public List<Job> getWorkflowExecutionsForTenant(long tenantPid) {
        String url = constructUrl(tenantPid);
        return get("getWorkflowExecutionsForTenant", url, List.class);
    }
}
