package com.latticeengines.pls.service;

import com.latticeengines.domain.exposed.camille.featureflags.FeatureFlagValueMap;

public interface DefaultFeatureFlagProvider {

    FeatureFlagValueMap getDefaultFlags();
}
