package com.latticeengines.pls.workflow;

import java.util.List;

import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.dataplatform.HasApplicationId;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.pls.ProvenancePropertyName;
import com.latticeengines.domain.exposed.scoringapi.TransformDefinition;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.transform.TransformationGroup;
import com.latticeengines.domain.exposed.transform.TransformationPipeline;
import com.latticeengines.domain.exposed.workflow.Job;
import com.latticeengines.domain.exposed.workflow.JobStatus;
import com.latticeengines.pls.service.PlsFeatureFlagService;
import com.latticeengines.pls.service.WorkflowJobService;
import com.latticeengines.proxy.exposed.matchapi.ColumnMetadataProxy;

@Component
public abstract class WorkflowSubmitter {

    @Inject
    protected WorkflowJobService workflowJobService;

    @Inject
    protected ColumnMetadataProxy columnMetadataProxy;

    @Inject
    protected PlsFeatureFlagService plsFeatureFlagService;

    @Value("${yarn.pls.url}")
    protected String internalResourceHostPort;

    @Value("${common.microservice.url}")
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
        } catch (Exception ignore) {
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
                return TransformationPipeline.getStandardTransforms();
            }
            return TransformationPipeline.getTransforms(transformationGroup);
        }
        return modelingEventTable.getRealTimeTransformationMetadata().getValue();
    }

    protected static String getTransformationGroupNameForModelSummary(ModelSummary modelSummary) {
        String transformationGroupName = modelSummary.getModelSummaryConfiguration()
                .getString(ProvenancePropertyName.TransformationGroupName, null);
        if (transformationGroupName == null) {
            transformationGroupName = modelSummary.getTransformationGroupName();
        }
        if (transformationGroupName == null) {
            throw new LedpException(LedpCode.LEDP_18108, new String[] { modelSummary.getId() });
        }
        return transformationGroupName;
    }

}
