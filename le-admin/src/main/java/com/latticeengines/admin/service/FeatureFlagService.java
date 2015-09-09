package com.latticeengines.admin.service;

import com.latticeengines.domain.exposed.camille.featureflags.FeatureFlagDefinition;
import com.latticeengines.domain.exposed.camille.featureflags.FeatureFlagDefinitionMap;
import com.latticeengines.domain.exposed.camille.featureflags.FeatureFlagValueMap;

public interface FeatureFlagService {

    void undefineFlag(String id);

    void defineFlag(String id, FeatureFlagDefinition definition);

    FeatureFlagDefinition getDefinition(String id);

    FeatureFlagDefinitionMap getDefinitions();

    void setFlag(String tenantId, String flagId, boolean value);

    void removeFlag(String tenantId, String flagId);

    FeatureFlagValueMap getFlags(String tenantId);

}
