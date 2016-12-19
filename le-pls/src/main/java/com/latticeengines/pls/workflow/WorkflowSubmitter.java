package com.latticeengines.pls.workflow;

import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.admin.LatticeFeatureFlag;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.camille.featureflags.FeatureFlagValueMap;
import com.latticeengines.domain.exposed.dataplatform.HasApplicationId;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.transform.TransformationGroup;
import com.latticeengines.domain.exposed.workflow.Job;
import com.latticeengines.domain.exposed.workflow.JobStatus;
import com.latticeengines.pls.service.WorkflowJobService;
import com.latticeengines.pls.service.impl.TenantConfigServiceImpl;
import com.latticeengines.proxy.exposed.matchapi.ColumnMetadataProxy;
import com.latticeengines.security.exposed.util.MultiTenantContext;

@Component
public abstract class WorkflowSubmitter {

    @Autowired
    protected WorkflowJobService workflowJobService;

    @Autowired
    protected TenantConfigServiceImpl tenantConfigService;

    @Autowired
    protected ColumnMetadataProxy columnMetadataProxy;

    @Value("${common.test.pls.url}")
    protected String internalResourceHostPort;

    @Value("${common.test.microservice.url}")
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

    public TransformationGroup getTransformationGroupFromZK() {
        TransformationGroup transformationGroup = TransformationGroup.STANDARD;

        FeatureFlagValueMap flags = tenantConfigService.getFeatureFlags(MultiTenantContext.getCustomerSpace()
                .toString());
        if (flags.containsKey(LatticeFeatureFlag.ENABLE_POC_TRANSFORM.getName())
                && Boolean.TRUE.equals(flags.get(LatticeFeatureFlag.ENABLE_POC_TRANSFORM.getName()))) {
            transformationGroup = TransformationGroup.ALL;
        }
        return transformationGroup;
    }

    public boolean isV2ProfilingEnabled() {
        FeatureFlagValueMap flags = tenantConfigService.getFeatureFlags(MultiTenantContext.getCustomerSpace()
                .toString());
        return flags.containsKey(LatticeFeatureFlag.ENABLE_DATA_PROFILING_V2.getName())
                && Boolean.TRUE.equals(flags.get(LatticeFeatureFlag.ENABLE_DATA_PROFILING_V2.getName()));
    }

    public boolean useDnBFlagFromZK() {

        FeatureFlagValueMap flags = tenantConfigService.getFeatureFlags(MultiTenantContext.getCustomerSpace()
                .toString());
        if (flags.containsKey(LatticeFeatureFlag.USE_DNB_RTS_AND_MODELING.getName())
                && Boolean.TRUE.equals(flags.get(LatticeFeatureFlag.USE_DNB_RTS_AND_MODELING.getName()))) {
            return true;
        }
        return false;
    }

    protected String getComplatibleDataCloudVersionFromModelSummary(ModelSummary summary) {
        String dataCloudVersion = "1.0.0";
        if (summary != null) {
            dataCloudVersion = summary.getDataCloudVersion();
            if (StringUtils.isNotEmpty(dataCloudVersion)) {
                dataCloudVersion = columnMetadataProxy.latestVersion(dataCloudVersion).getVersion();
            }
        }
        return dataCloudVersion;
    }
}
