package com.latticeengines.pls.workflow;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.dataplatform.HasApplicationId;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.workflow.WorkflowStatus;
import com.latticeengines.proxy.exposed.workflowapi.WorkflowProxy;
import com.latticeengines.security.exposed.util.SecurityContextUtils;

@Component
public abstract class WorkflowSubmitter {
    @Autowired
    protected WorkflowProxy workflowProxy;

    @Value("${pls.api.hostport}")
    protected String internalResourceHostPort;

    @Value("${pls.microservice.rest.endpoint.hostport}")
    protected String microserviceHostPort;

    protected CustomerSpace getCustomerSpace() {
        Tenant tenant = SecurityContextUtils.getTenant();
        if (tenant == null) {
            throw new RuntimeException("No tenant in context");
        }
        return CustomerSpace.parse(tenant.getId());
    }

    protected boolean hasRunningWorkflow(HasApplicationId entity) {
        String appId = entity.getApplicationId();
        if (appId == null) {
            return false;
        }
        WorkflowStatus status = null;
        try {
            status = workflowProxy.getWorkflowStatusFromApplicationId(appId);
        } catch (Exception e) {
            // Ignore any errors since this means that any associated workflow
            // must be problematic so let it continue
        }
        return status != null && status.getStatus().isRunning();
    }
}
