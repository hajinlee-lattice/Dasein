package com.latticeengines.pls.service;

import com.latticeengines.domain.exposed.admin.CRMTopology;
import com.latticeengines.domain.exposed.admin.TenantDocument;
import com.latticeengines.domain.exposed.camille.featureflags.FeatureFlagValueMap;

public interface TenantConfigService {

    CRMTopology getTopology(String tenantId);

    String getDLRestServiceAddress(String tenantId);

    TenantDocument getTenantDocument(String tenantId);

    FeatureFlagValueMap getFeatureFlags(String tenantId);
}
