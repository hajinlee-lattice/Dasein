package com.latticeengines.pls.entitymanager.impl.microservice;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.dataplatform.JobStatus;
import com.latticeengines.domain.exposed.workflow.WorkflowConfiguration;

@Component("restApiProxy")
public class RestApiProxy implements WorkflowProxy, JobProxy {

    @Value("${pls.microservice.rest.endpoint.hostport}")
    private String microserviceHostPort;

    @Override
    public ApplicationId submitWorkflow(WorkflowConfiguration config) {
        return null;
    }

    @Override
    public JobStatus getJobStatus(String applicationId) {
        return null;
    }

}
