package com.latticeengines.admin.service;

import java.util.Collection;
import java.util.Map;

import com.latticeengines.domain.exposed.admin.SerializableDocumentDirectory;
import com.latticeengines.domain.exposed.admin.SpaceConfiguration;
import com.latticeengines.domain.exposed.admin.TenantDocument;
import com.latticeengines.domain.exposed.admin.TenantRegistration;
import com.latticeengines.domain.exposed.camille.DocumentDirectory;
import com.latticeengines.domain.exposed.camille.bootstrap.BootstrapState;

public interface TenantService {

    boolean createTenant(String contractId, String tenantId, TenantRegistration tenantRegistration);

    Collection<TenantDocument> getTenants(String contractId);

    boolean deleteTenant(String contractId, String tenantId);

    TenantDocument getTenant(String contractId, String tenantId);

    BootstrapState getTenantServiceState(String contractId, String tenantId, String serviceName);

    BootstrapState getTenantOverallState(String contractId, String tenantId);

    boolean bootstrap(String contractId, String tenantId, String serviceName, Map<String, String> properties);

    SerializableDocumentDirectory getTenantServiceConfig(String contractId, String tenantId, String serviceName);

    SpaceConfiguration getDefaultSpaceConfig();

    DocumentDirectory getSpaceConfigSchema();

    boolean setupSpaceConfiguration(String contractId, String tenantId, SpaceConfiguration spaceConfig);

    boolean danteIsEnabled(String contracId, String tenantId);
}
