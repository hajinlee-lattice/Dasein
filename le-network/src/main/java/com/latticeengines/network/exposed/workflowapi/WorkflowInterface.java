package com.latticeengines.network.exposed.workflowapi;

import java.util.List;

import com.latticeengines.domain.exposed.workflow.Job;

public interface WorkflowInterface {

    void stopWorkflow(String customerSpace, String workflowId);

    List<Job> getWorkflowJobs(String customerSpace, List<String> jobIds, List<String> types, Boolean includeDetails,
            Boolean hasParentId);

    Job updateParentJobId(String customerSpace, List<String> jobIds, String parentJobId);
}
