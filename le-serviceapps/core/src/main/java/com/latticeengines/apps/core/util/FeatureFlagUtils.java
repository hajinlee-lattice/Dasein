package com.latticeengines.apps.core.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.domain.exposed.admin.LatticeFeatureFlag;
import com.latticeengines.domain.exposed.camille.featureflags.FeatureFlagValueMap;
import com.latticeengines.domain.exposed.transform.TransformationGroup;

public class FeatureFlagUtils {

    private static final Logger log = LoggerFactory.getLogger(FeatureFlagUtils.class);

    @SuppressWarnings("deprecation")
    public static TransformationGroup getTransformationGroupFromZK(FeatureFlagValueMap flags) {
        TransformationGroup transformationGroup = TransformationGroup.STANDARD;
        if (flags.containsKey(LatticeFeatureFlag.ENABLE_POC_TRANSFORM.getName())
                && Boolean.TRUE.equals(flags.get(LatticeFeatureFlag.ENABLE_POC_TRANSFORM.getName()))) {
            transformationGroup = TransformationGroup.ALL;
        }
        return transformationGroup;
    }

    public static boolean isMatchDebugEnabled(FeatureFlagValueMap flags) {
        try {
            return flags.containsKey(LatticeFeatureFlag.ENABLE_MATCH_DEBUG.getName())
                    && Boolean.TRUE.equals(flags.get(LatticeFeatureFlag.ENABLE_MATCH_DEBUG.getName()));
        } catch (Exception e) {
            log.error("Error when retrieving " + LatticeFeatureFlag.ENABLE_MATCH_DEBUG.getName() + " feature flag!", e);
            return false;
        }
    }

    public static boolean isEntityMatchEnabled(FeatureFlagValueMap flags) {
        try {
            return flags.containsKey(LatticeFeatureFlag.ENABLE_ENTITY_MATCH.getName())
                    && Boolean.TRUE.equals(flags.get(LatticeFeatureFlag.ENABLE_ENTITY_MATCH.getName()));
        } catch (Exception e) {
            log.error("Error when retrieving " + LatticeFeatureFlag.ENABLE_ENTITY_MATCH.getName() + " feature flag!", e);
            return false;
        }
    }

    public static boolean isAlwaysOnCampaign(FeatureFlagValueMap flags) {
        try {
            return flags.containsKey(LatticeFeatureFlag.ALWAYS_ON_CAMPAIGNS.getName())
                    && Boolean.TRUE.equals(flags.get(LatticeFeatureFlag.ALWAYS_ON_CAMPAIGNS.getName()));
        } catch (Exception e) {
            log.error("Error when retrieving " + LatticeFeatureFlag.ALWAYS_ON_CAMPAIGNS.getName() + " feature flag!", e);
            return false;
        }
    }

    public static boolean isApsImputationEnabled(FeatureFlagValueMap flags) {
        try {
            return flags.containsKey(LatticeFeatureFlag.ENABLE_APS_IMPUTATION.getName())
                    && Boolean.TRUE.equals(flags.get(LatticeFeatureFlag.ENABLE_APS_IMPUTATION.getName()));
        } catch (Exception e) {
            log.error("Error when retrieving " + LatticeFeatureFlag.ENABLE_APS_IMPUTATION.getName() + " feature flag!",
                    e);
            return false;
        }
    }

    @SuppressWarnings("deprecation")
    public static boolean isFuzzyMatchEnabled(FeatureFlagValueMap flags) {
        return flags.containsKey(LatticeFeatureFlag.ENABLE_FUZZY_MATCH.getName())
                && Boolean.TRUE.equals(flags.get(LatticeFeatureFlag.ENABLE_FUZZY_MATCH.getName()));
    }

    @SuppressWarnings("deprecation")
    public static boolean isV2ProfilingEnabled(FeatureFlagValueMap flags) {
        return flags.containsKey(LatticeFeatureFlag.ENABLE_DATA_PROFILING_V2.getName())
                && Boolean.TRUE.equals(flags.get(LatticeFeatureFlag.ENABLE_DATA_PROFILING_V2.getName()));
    }

    @SuppressWarnings("deprecation")
    public static boolean useDnBFlagFromZK(FeatureFlagValueMap flags) {
        if (flags.containsKey(LatticeFeatureFlag.USE_DNB_RTS_AND_MODELING.getName())
                && Boolean.TRUE.equals(flags.get(LatticeFeatureFlag.USE_DNB_RTS_AND_MODELING.getName()))) {
            return true;
        }
        return false;
    }
}
