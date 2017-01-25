package com.latticeengines.pls.service.impl;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.admin.LatticeFeatureFlag;
import com.latticeengines.domain.exposed.camille.featureflags.FeatureFlagValueMap;
import com.latticeengines.domain.exposed.transform.TransformationGroup;
import com.latticeengines.pls.service.PlsFeatureFlagService;
import com.latticeengines.security.exposed.util.MultiTenantContext;

@Component("plsFeatureFlagService")
public class PlsFeatureFlagServiceImpl implements PlsFeatureFlagService {

    @Autowired
    protected TenantConfigServiceImpl tenantConfigService;

    @Override
    public TransformationGroup getTransformationGroupFromZK() {
        TransformationGroup transformationGroup = TransformationGroup.STANDARD;

        FeatureFlagValueMap flags = tenantConfigService
                .getFeatureFlags(MultiTenantContext.getCustomerSpace().toString());
        if (flags.containsKey(LatticeFeatureFlag.ENABLE_POC_TRANSFORM.getName())
                && Boolean.TRUE.equals(flags.get(LatticeFeatureFlag.ENABLE_POC_TRANSFORM.getName()))) {
            transformationGroup = TransformationGroup.ALL;
        }
        return transformationGroup;
    }

    @Override
    public boolean isV2ProfilingEnabled() {
        FeatureFlagValueMap flags = tenantConfigService
                .getFeatureFlags(MultiTenantContext.getCustomerSpace().toString());
        return flags.containsKey(LatticeFeatureFlag.ENABLE_DATA_PROFILING_V2.getName())
                && Boolean.TRUE.equals(flags.get(LatticeFeatureFlag.ENABLE_DATA_PROFILING_V2.getName()));
    }

    @Override
    public boolean isFuzzyMatchEnabled() {
        FeatureFlagValueMap flags = tenantConfigService
                .getFeatureFlags(MultiTenantContext.getCustomerSpace().toString());
        return flags.containsKey(LatticeFeatureFlag.ENABLE_FUZZY_MATCH.getName())
                && Boolean.TRUE.equals(flags.get(LatticeFeatureFlag.ENABLE_FUZZY_MATCH.getName()));
    }

    @Override
    public boolean useDnBFlagFromZK() {
        FeatureFlagValueMap flags = tenantConfigService
                .getFeatureFlags(MultiTenantContext.getCustomerSpace().toString());
        if (flags.containsKey(LatticeFeatureFlag.USE_DNB_RTS_AND_MODELING.getName())
                && Boolean.TRUE.equals(flags.get(LatticeFeatureFlag.USE_DNB_RTS_AND_MODELING.getName()))) {
            return true;
        }
        return false;
    }
}
