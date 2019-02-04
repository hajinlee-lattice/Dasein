package com.latticeengines.testframework.exposed.service;

import java.util.List;
import java.util.Map;

import org.springframework.web.client.RestTemplate;

import com.latticeengines.domain.exposed.admin.LatticeProduct;
import com.latticeengines.domain.exposed.camille.featureflags.FeatureFlagValueMap;
import com.latticeengines.domain.exposed.pls.UserDocument;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.testframework.exposed.rest.LedpResponseErrorHandler;

public interface GlobalAuthTestBed {

    RestTemplate getMagicRestTemplate();

    RestTemplate getRestTemplate();

    LedpResponseErrorHandler getErrorHandler();

    void bootstrap(Integer numTenants);

    Tenant bootstrapForProduct(LatticeProduct product);

    Tenant bootstrapForProduct(String tenantIdentifier, LatticeProduct product);

    Tenant bootstrapForProduct(LatticeProduct product, Map<String, Boolean> featureFlagMap);

    Tenant bootstrapForProduct(String tenantIdentifier, LatticeProduct product, Map<String, Boolean> featureFlagMap);

    Tenant bootstrapForProduct(LatticeProduct product, String jsonFileName);

    List<Tenant> getTestTenants();

    Tenant addExtraTestTenant(String tenantName);

    Tenant getMainTestTenant();

    void setMainTestTenant(Tenant tenant);

    Tenant useExistingTenantAsMain(String tenantName);

    UserDocument loginAndAttach(String username, String password, Tenant tenant);

    UserDocument getCurrentUser();

    FeatureFlagValueMap getFeatureFlags();

    void switchToSuperAdmin();

    void switchToInternalAdmin();

    void switchToInternalUser();

    void switchToExternalAdmin();

    void switchToExternalUser();

    void switchToThirdPartyUser();

    void switchToSuperAdmin(Tenant tenant);

    void switchToInternalAdmin(Tenant tenant);

    void switchToInternalUser(Tenant tenant);

    void switchToExternalAdmin(Tenant tenant);

    void switchToExternalUser(Tenant tenant);

    void switchToThirdPartyUser(Tenant tenant);

    void createTenant(Tenant tenant);

    void deleteTenant(Tenant tenant);

    void cleanupS3();

    void cleanupDlZk();

    void cleanupPlsHdfs();

    void cleanupRedshift();

    void overwriteFeatureFlag(Tenant tenant, String featureFlagName, boolean value);

    void excludeTestTenantsForCleanup(List<Tenant> tenants);

    void loginAD();
}
