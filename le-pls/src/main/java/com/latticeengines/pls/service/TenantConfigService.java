package com.latticeengines.pls.service;

import java.util.List;

import com.latticeengines.domain.exposed.admin.CRMTopology;
import com.latticeengines.domain.exposed.admin.LatticeProduct;
import com.latticeengines.domain.exposed.admin.TenantDocument;
import com.latticeengines.domain.exposed.camille.featureflags.FeatureFlagValueMap;

public interface TenantConfigService {

    CRMTopology getTopology(String tenantId);

    String getDLRestServiceAddress(String tenantId);

    String removeDLRestServicePart(String dlRestServiceUrl);

    TenantDocument getTenantDocument(String tenantId);

    FeatureFlagValueMap getFeatureFlags(String tenantId);

    List<LatticeProduct> getProducts(String tenantId);
}
