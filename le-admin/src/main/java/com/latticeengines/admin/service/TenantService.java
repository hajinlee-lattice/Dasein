package com.latticeengines.admin.service;

import java.util.Collection;
import java.util.Map;

import com.latticeengines.domain.exposed.admin.SerializableDocumentDirectory;
import com.latticeengines.domain.exposed.admin.SpaceConfiguration;
import com.latticeengines.domain.exposed.admin.TenantDocument;
import com.latticeengines.domain.exposed.admin.TenantRegistration;
import com.latticeengines.domain.exposed.camille.DocumentDirectory;
import com.latticeengines.domain.exposed.camille.bootstrap.BootstrapState;
import com.latticeengines.domain.exposed.camille.lifecycle.TenantInfo;

public interface TenantService {

    boolean createTenant(String contractId, String tenantId, TenantRegistration tenantRegistration, String userName);

    boolean createTenantV2(String contractId, String tenantId, TenantRegistration tenantRegistration,  String userName);

    Collection<TenantDocument> getTenants(String contractId);

    Collection<TenantDocument> getTenantsInCache(String contractId);

    boolean deleteTenant(final String userName, String contractId, String tenantId, boolean uninstallComponent);

    TenantDocument getTenant(String contractId, String tenantId);

    BootstrapState getTenantServiceState(String contractId, String tenantId, String serviceName);

    BootstrapState getTenantServiceStateInCache(String contractId, String tenantId, String serviceName);

    BootstrapState getTenantOverallState(String contractId, String tenantId, TenantDocument doc);

    boolean bootstrap(String contractId, String tenantId, String serviceName, Map<String, String> properties);

    SerializableDocumentDirectory getTenantServiceConfig(String contractId, String tenantId, String serviceName);

    SpaceConfiguration getDefaultSpaceConfig();

    DocumentDirectory getSpaceConfigSchema();

    boolean setupSpaceConfiguration(String contractId, String tenantId, SpaceConfiguration spaceConfig);

    boolean danteIsEnabled(String contracId, String tenantId);

    boolean updateTenantInfo(String contractId, String tenantId, TenantInfo tenantInfo);
}
