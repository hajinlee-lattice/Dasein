package com.latticeengines.testframework.security;

import java.util.List;

import org.springframework.web.client.ResponseErrorHandler;
import org.springframework.web.client.RestTemplate;

import com.latticeengines.domain.exposed.admin.LatticeProduct;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.testframework.rest.LedpResponseErrorHandler;

public interface GlobalAuthTestBed {

    RestTemplate getMagicRestTemplate();
    RestTemplate getRestTemplate();
    LedpResponseErrorHandler getErrorHandler();

    void bootstrap(Integer numTenants);
    void bootstrapForProduct(LatticeProduct product);

    List<Tenant> getTestTenants();
    Tenant addExtraTestTenant(String tenantName);
    Tenant getMainTestTenant();
    void setMainTestTenant(Tenant tenant);


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

    void cleanup();
}
