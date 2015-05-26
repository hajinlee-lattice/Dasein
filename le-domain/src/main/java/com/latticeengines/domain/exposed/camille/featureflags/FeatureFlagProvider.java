package com.latticeengines.domain.exposed.camille.featureflags;

import com.latticeengines.domain.exposed.camille.CustomerSpace;

public interface FeatureFlagProvider {
    boolean isEnabled(CustomerSpace space, String id);

    FeatureFlagDefinition getDefinition(String id);

    void setEnabled(CustomerSpace space, String id, boolean enabled);

    void setDefinition(String id, FeatureFlagDefinition definition);

    void remove(String id);
}
