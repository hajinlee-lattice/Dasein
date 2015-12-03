package com.latticeengines.pls.entitymanager.impl.microservice;

import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

import com.latticeengines.common.exposed.util.YarnUtils;
import com.latticeengines.domain.exposed.api.AppSubmission;
import com.latticeengines.domain.exposed.dataplatform.JobStatus;
import com.latticeengines.domain.exposed.workflow.Job;
import com.latticeengines.domain.exposed.workflow.WorkflowConfiguration;
import com.latticeengines.domain.exposed.workflow.WorkflowStatus;
import com.latticeengines.security.exposed.MagicAuthenticationHeaderHttpRequestInterceptor;

@Component("restApiProxy")
public class RestApiProxy implements WorkflowProxy, JobProxy {

    private RestTemplate restTemplate = new RestTemplate();

    @Value("${pls.microservice.rest.endpoint.hostport}")
    private String microserviceHostPort;

    public RestApiProxy() {
        restTemplate.getInterceptors().add(new MagicAuthenticationHeaderHttpRequestInterceptor());
    }

    @Override
    public ApplicationId submitWorkflow(final WorkflowConfiguration config) {
        try {
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

    // @Override
    public Job getWorkflowExecution(String workflowId) {
        try {
            return restTemplate.getForObject(constructUrl("workflowapi", "job/" + workflowId), Job.class);
        } catch (Exception e) {
            throw new RuntimeException("getWorkflowExecution: Remote call failure", e);
        }
    }

    // @Override
    @SuppressWarnings("unchecked")
    public List<Job> getWorkflowExecutionsForTenant(long tenantPid) {
        try {
            return restTemplate.getForObject(constructUrl("workflowapi", "jobs/" + tenantPid), List.class);
        } catch (Exception e) {
            throw new RuntimeException("getWorkflowExecutionsForTenant: Remote call failure", e);
        }
    }

    private String constructUrl(String context) {
        return constructUrl(context, null);
    }

    private String constructUrl(String context, String path) {
        String end = context;
        if (path != null) {
            end = combine(context, path);
        }
        return combine(microserviceHostPort, end);
    }

    private String combine(String... parts) {
        for (int i = 0; i < parts.length; ++i) {
            String part = parts[i];
            if (i != 0) {
                if (part.startsWith("/")) {
                    part = part.substring(1);
                }
            }

            if (i != parts.length - 1) {
                if (part.endsWith("/")) {
                    part = part.substring(0, part.length() - 2);
                }
            }
        }
        return StringUtils.join(parts, "/");
    }

    @Override
    public JobStatus getJobStatus(String applicationId) {
        return null;
    }
}
