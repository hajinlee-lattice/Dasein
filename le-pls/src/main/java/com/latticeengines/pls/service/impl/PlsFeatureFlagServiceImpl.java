package com.latticeengines.pls.service.impl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.admin.LatticeFeatureFlag;
import com.latticeengines.domain.exposed.camille.featureflags.FeatureFlagValueMap;
import com.latticeengines.domain.exposed.transform.TransformationGroup;
import com.latticeengines.pls.service.PlsFeatureFlagService;

@Component("plsFeatureFlagService")
public class PlsFeatureFlagServiceImpl implements PlsFeatureFlagService {

    private static final Logger log = LoggerFactory.getLogger(PlsFeatureFlagServiceImpl.class);

    @Autowired
    protected TenantConfigServiceImpl tenantConfigService;

    @Override
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

    @Override
    public boolean isV2ProfilingEnabled() {
        FeatureFlagValueMap flags = tenantConfigService.getFeatureFlags(MultiTenantContext.getCustomerSpace()
                .toString());
        return flags.containsKey(LatticeFeatureFlag.ENABLE_DATA_PROFILING_V2.getName())
                && Boolean.TRUE.equals(flags.get(LatticeFeatureFlag.ENABLE_DATA_PROFILING_V2.getName()));
    }

    @Override
    public boolean isFuzzyMatchEnabled() {
        FeatureFlagValueMap flags = tenantConfigService.getFeatureFlags(MultiTenantContext.getCustomerSpace()
                .toString());
        return flags.containsKey(LatticeFeatureFlag.ENABLE_FUZZY_MATCH.getName())
                && Boolean.TRUE.equals(flags.get(LatticeFeatureFlag.ENABLE_FUZZY_MATCH.getName()));
    }

    @Override
    public boolean useDnBFlagFromZK() {
        FeatureFlagValueMap flags = tenantConfigService.getFeatureFlags(MultiTenantContext.getCustomerSpace()
                .toString());
        if (flags.containsKey(LatticeFeatureFlag.USE_DNB_RTS_AND_MODELING.getName())
                && Boolean.TRUE.equals(flags.get(LatticeFeatureFlag.USE_DNB_RTS_AND_MODELING.getName()))) {
            return true;
        }
        return false;
    }

    @Override
    public boolean isMatchDebugEnabled() {
        try {
            FeatureFlagValueMap flags = tenantConfigService.getFeatureFlags(MultiTenantContext.getCustomerSpace()
                    .toString());
            return flags.containsKey(LatticeFeatureFlag.ENABLE_MATCH_DEBUG.getName())
                    && Boolean.TRUE.equals(flags.get(LatticeFeatureFlag.ENABLE_MATCH_DEBUG.getName()));
        } catch (Exception e) {
            log.error("Error when retrieving " + LatticeFeatureFlag.ENABLE_MATCH_DEBUG.getName() + " feature flag!", e);
            return false;
        }

    }
}
