package com.latticeengines.pls.entitymanager.impl.microservice;

import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.YarnUtils;
import com.latticeengines.domain.exposed.api.AppSubmission;
import com.latticeengines.domain.exposed.dataplatform.JobStatus;
import com.latticeengines.domain.exposed.workflow.Job;
import com.latticeengines.domain.exposed.workflow.WorkflowConfiguration;
import com.latticeengines.domain.exposed.workflow.WorkflowStatus;
import com.latticeengines.security.exposed.util.BaseRestApiProxy;

@Component("restApiProxy")
public class RestApiProxy extends BaseRestApiProxy implements WorkflowProxy, JobProxy {

    private static final Log log = LogFactory.getLog(RestApiProxy.class);

    @Value("${pls.microservice.rest.endpoint.hostport}")
    private String microserviceHostPort;

    @Override
    public String getRestApiHostPort() {
        return microserviceHostPort;
    }

    @Override
    public ApplicationId submitWorkflow(final WorkflowConfiguration config) {
        try {
            log.info("Submitting workflow with config: " + config.toString());
            AppSubmission submission = restTemplate.postForObject(constructUrl("workflowapi", "workflows/"), config,
                    AppSubmission.class);
            return YarnUtils.appIdFromString(submission.getApplicationIds().get(0));
        } catch (Exception e) {
            throw new RuntimeException("submitWorkflow: Remote call failure", e);
        }
    }

    @Override
    public WorkflowStatus getWorkflowStatusFromApplicationId(String applicationId) {
        try {
            return restTemplate.getForObject(constructUrl("workflowapi", "workflows/yarnapps/status/" + applicationId),
                    WorkflowStatus.class);
        } catch (Exception e) {
            throw new RuntimeException("getWorkflowStatusFromApplicationId: Remote call failure", e);
        }
    }

     @Override
    public Job getWorkflowExecution(String workflowId) {
        try {
            return restTemplate.getForObject(constructUrl("workflowapi", "workflows/job/" + workflowId), Job.class);
        } catch (Exception e) {
            throw new RuntimeException("getWorkflowExecution: Remote call failure", e);
        }
    }

     @Override
    @SuppressWarnings("unchecked")
    public List<Job> getWorkflowExecutionsForTenant(long tenantPid) {
        try {
            return restTemplate.getForObject(constructUrl("workflowapi", "workflows/jobs/" + tenantPid), List.class);
        } catch (Exception e) {
            throw new RuntimeException("getWorkflowExecutionsForTenant: Remote call failure", e);
        }
    }

    @Override
    public JobStatus getJobStatus(String applicationId) {
        return null;
    }

}
