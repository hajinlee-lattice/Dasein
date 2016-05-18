package com.latticeengines.pls.workflow;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.admin.LatticeFeatureFlag;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.camille.featureflags.FeatureFlagValueMap;
import com.latticeengines.domain.exposed.dataplatform.HasApplicationId;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.transform.TransformationGroup;
import com.latticeengines.domain.exposed.workflow.Job;
import com.latticeengines.domain.exposed.workflow.JobStatus;
import com.latticeengines.pls.service.WorkflowJobService;
import com.latticeengines.pls.service.impl.TenantConfigServiceImpl;
import com.latticeengines.security.exposed.util.MultiTenantContext;

@Component
public abstract class WorkflowSubmitter {

    @Autowired
    protected WorkflowJobService workflowJobService;

    @Autowired
    protected TenantConfigServiceImpl tenantConfigService;

    @Value("${pls.api.hostport}")
    protected String internalResourceHostPort;

    @Value("${pls.microservice.rest.endpoint.hostport}")
    protected String microserviceHostPort;

    protected CustomerSpace getCustomerSpace() {
        Tenant tenant = MultiTenantContext.getTenant();
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
        JobStatus status = null;
        try {
            status = workflowJobService.getJobStatusFromApplicationId(appId);
        } catch (Exception e) {
            // Ignore any errors since this means that any associated workflow
            // must be problematic so let it continue
        }
        return status != null && !Job.TERMINAL_JOB_STATUS.contains(status);
    }

    public TransformationGroup gettransformationGroupFromZK() {
        TransformationGroup transformationGroup = TransformationGroup.STANDARD;

        FeatureFlagValueMap flags = tenantConfigService.getFeatureFlags(MultiTenantContext.getCustomerSpace()
                .toString());
        if (flags.containsKey(LatticeFeatureFlag.ENABLE_POC_TRANSFORM.getName())
                && Boolean.TRUE.equals(flags.get(LatticeFeatureFlag.ENABLE_POC_TRANSFORM.getName()))) {
            transformationGroup = TransformationGroup.ALL;
        }
        return transformationGroup;
    }
}
