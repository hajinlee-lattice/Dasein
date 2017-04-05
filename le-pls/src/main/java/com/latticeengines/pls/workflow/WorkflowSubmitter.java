package com.latticeengines.pls.workflow;

import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.dataplatform.HasApplicationId;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.scoringapi.TransformDefinition;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.transform.TransformationGroup;
import com.latticeengines.domain.exposed.transform.TransformationPipeline;
import com.latticeengines.domain.exposed.workflow.Job;
import com.latticeengines.domain.exposed.workflow.JobStatus;
import com.latticeengines.pls.service.PlsFeatureFlagService;
import com.latticeengines.pls.service.WorkflowJobService;
import com.latticeengines.proxy.exposed.matchapi.ColumnMetadataProxy;
import com.latticeengines.security.exposed.util.MultiTenantContext;

@Component
public abstract class WorkflowSubmitter {

    @Autowired
    protected WorkflowJobService workflowJobService;

    @Autowired
    protected ColumnMetadataProxy columnMetadataProxy;
    
    @Autowired
    protected PlsFeatureFlagService plsFeatureFlagService;

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

    protected String getLatestDataCloudVersion() {
        return columnMetadataProxy.latestVersion(null).getVersion();
    }

    protected List<TransformDefinition> getTransformDefinitions(Table modelingEventTable,
            TransformationGroup transformationGroup) {
        if (transformationGroup != TransformationGroup.NONE
                && modelingEventTable.getRealTimeTransformationMetadata().getValue().isEmpty()) {
            if (transformationGroup == TransformationGroup.STANDARD) {
                return TransformationPipeline.getStandardTransformsV1();
            }
            return TransformationPipeline.getTransforms(transformationGroup);
        }
        return modelingEventTable.getRealTimeTransformationMetadata().getValue();
    }

}
