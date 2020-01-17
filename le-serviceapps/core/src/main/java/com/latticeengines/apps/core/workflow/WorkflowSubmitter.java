package com.latticeengines.apps.core.workflow;

import java.util.List;

import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.apps.core.service.WorkflowJobService;
import com.latticeengines.apps.core.util.ArtifactUtils;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.dataplatform.HasApplicationId;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.Artifact;
import com.latticeengines.domain.exposed.metadata.ArtifactType;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.scoringapi.TransformDefinition;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.transform.TransformationGroup;
import com.latticeengines.domain.exposed.transform.TransformationPipeline;
import com.latticeengines.domain.exposed.workflow.Job;
import com.latticeengines.domain.exposed.workflow.JobStatus;
import com.latticeengines.proxy.exposed.matchapi.ColumnMetadataProxy;

@Component
public abstract class WorkflowSubmitter {

    private static final Logger log = LoggerFactory.getLogger(WorkflowSubmitter.class);

    @Inject
    protected WorkflowJobService workflowJobService;

    @Inject
    private ColumnMetadataProxy columnMetadataProxy;

    @Inject
    protected Configuration yarnConfiguration;

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
        } catch (Exception e) {
            log.warn("Failed to get job status from app id. But it is ignored, " +
                    "since this means that any associated workflow must be problematic so let it continue", e);
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

    protected Artifact getPivotArtifact(String moduleName, final String pivotFileName) {
        Artifact pivotArtifact = null;

        if (StringUtils.isNotEmpty(moduleName) && StringUtils.isNotEmpty(pivotFileName)) {
            List<Artifact> pivotArtifacts = ArtifactUtils.getArtifacts(moduleName, ArtifactType.PivotMapping,
                    yarnConfiguration);
            pivotArtifact = pivotArtifacts.stream().filter(artifact -> artifact.getName().equals(pivotFileName))
                    .findFirst().orElse(null);
            if (pivotArtifact == null) {
                throw new LedpException(LedpCode.LEDP_28026, new String[] { pivotFileName, moduleName });
            }
        }
        return pivotArtifact;
    }

}
