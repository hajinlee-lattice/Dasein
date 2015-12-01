package com.latticeengines.pls.entitymanager.impl.microservice;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.workflow.WorkflowConfiguration;

@Component("restApiProxy")
public class RestApiProxy implements WorkflowProxy {

    @Value("${pls.microservice.rest.endpoint.hostport}")
    private String microserviceHostPort;

    @Override
    public ApplicationId submitWorkflow(WorkflowConfiguration config) {
        return null;
    }

}
