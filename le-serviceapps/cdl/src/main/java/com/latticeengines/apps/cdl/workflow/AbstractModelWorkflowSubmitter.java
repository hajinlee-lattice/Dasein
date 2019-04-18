package com.latticeengines.apps.cdl.workflow;

import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;

import com.latticeengines.apps.cdl.provision.impl.CDLComponent;
import com.latticeengines.apps.core.util.FeatureFlagUtils;
import com.latticeengines.apps.core.workflow.WorkflowSubmitter;
import com.latticeengines.baton.exposed.service.BatonService;
import com.latticeengines.camille.exposed.Camille;
import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.camille.Path;
import com.latticeengines.domain.exposed.camille.featureflags.FeatureFlagValueMap;
import com.latticeengines.domain.exposed.pls.ModelingParameters;
import com.latticeengines.domain.exposed.transform.TransformationGroup;
import com.latticeengines.proxy.exposed.matchapi.ColumnMetadataProxy;

public abstract class AbstractModelWorkflowSubmitter extends WorkflowSubmitter {

    private static final Logger log = LoggerFactory.getLogger(AbstractModelWorkflowSubmitter.class);

    private static final String MODELING_PROFILING_VERSION = "ModelingProfilingVersion";
    private static final String MODELING_PROFILING_V2 = "v2";

    @Inject
    private ColumnMetadataProxy columnMetadataProxy;

    @Inject
    private BatonService batonService;

    @Value("${pls.modeling.validation.min.rows:300}")
    private long minRows;

    @Value("${pls.modeling.validation.min.eventrows:50}")
    private long minPositiveEvents;

    @Value("${pls..modeling.validation.min.negativerows:250}")
    private long minNegativeEvents;

    @Value("${pls.modelingservice.basedir}")
    private String modelingServiceHdfsBaseDir;

    @Value("${cdl.modeling.workflow.mem.mb}")
    protected int workflowMemMb;

    protected FeatureFlagValueMap getFeatureFlagValueMap() {
        return batonService.getFeatureFlags(MultiTenantContext.getCustomerSpace());
    }

    protected TransformationGroup getTransformationGroup(FeatureFlagValueMap flags) {
        return FeatureFlagUtils.getTransformationGroupFromZK(flags);
    }

    protected boolean isFuzzyMatchEnabled(FeatureFlagValueMap flags) {
        return FeatureFlagUtils.isFuzzyMatchEnabled(flags);
    }

    protected boolean isV2ProfilingEnabled() {
        return MODELING_PROFILING_V2.equals(getModelingProfilingVersion());
    }

    protected boolean isMatchDebugEnabled(FeatureFlagValueMap flags) {
        return FeatureFlagUtils.isMatchDebugEnabled(flags);
    }

    protected String getDataCloudVersion(ModelingParameters parameters, FeatureFlagValueMap flags) {
        if (StringUtils.isNotEmpty(parameters.getDataCloudVersion())) {
            return parameters.getDataCloudVersion();
        }
        if (FeatureFlagUtils.useDnBFlagFromZK(flags)) {
            // retrieve latest version from matchapi
            return columnMetadataProxy.latestVersion(null).getVersion();
        }
        return null;
    }

    private String getModelingProfilingVersion() {
        String profiling = MODELING_PROFILING_V2;
        try {
            Camille camille = CamilleEnvironment.getCamille();
            CustomerSpace customerSpace = MultiTenantContext.getCustomerSpace();
            Path docPath = PathBuilder.buildCustomerSpaceServicePath(CamilleEnvironment.getPodId(), customerSpace,
                    CDLComponent.componentName);
            docPath = docPath.append(MODELING_PROFILING_VERSION);
            if (camille.exists(docPath)) {
                profiling = camille.get(docPath).getData();
            }
        } catch (Exception ex) {
            log.error(String.format("Can not get tenant's modeling profining version from ZK, " //
                    + "defaulting to %s", profiling), ex);
        }
        return profiling;
    }
}
