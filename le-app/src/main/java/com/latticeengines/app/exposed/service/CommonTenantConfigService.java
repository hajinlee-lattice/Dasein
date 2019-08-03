package com.latticeengines.app.exposed.service;

import java.util.List;

import com.latticeengines.domain.exposed.admin.LatticeProduct;
import com.latticeengines.domain.exposed.admin.TenantDocument;
import com.latticeengines.domain.exposed.camille.featureflags.FeatureFlagValueMap;
import com.latticeengines.domain.exposed.pls.TenantConfiguration;

public interface CommonTenantConfigService {

    int getMaxPremiumLeadEnrichmentAttributes(String tenantId);

    List<LatticeProduct> getProducts(String tenantId);

    TenantDocument getTenantDocument(String tenantId);

    FeatureFlagValueMap getFeatureFlags(String tenantId);

    TenantConfiguration getTenantConfiguration();

    int getMaxPremiumLeadEnrichmentAttributesByLicense(String tenantId, String dataLicense);

    boolean isEntityMatchEnabled();

    boolean onlyEntityMatchGAEnabled();

}
