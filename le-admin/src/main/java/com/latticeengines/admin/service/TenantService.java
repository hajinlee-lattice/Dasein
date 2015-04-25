package com.latticeengines.admin.service;

import java.util.AbstractMap;
import java.util.List;
import java.util.Map;

import com.latticeengines.domain.exposed.admin.SerializableDocumentDirectory;
import com.latticeengines.domain.exposed.admin.TenantRegistration;
import com.latticeengines.domain.exposed.camille.bootstrap.BootstrapState;
import com.latticeengines.domain.exposed.camille.lifecycle.TenantInfo;

public interface TenantService {
    
    boolean createTenant(String contractId, String tenantId, TenantRegistration tenantRegistration);

    List<AbstractMap.SimpleEntry<String, TenantInfo>> getTenants(String contractId);

    boolean deleteTenant(String contractId, String tenantId);

    TenantInfo getTenant(String contractId, String tenantId);

    BootstrapState getTenantServiceState(String contractId, String tenantId, String serviceName);

    BootstrapState getTenantOverallState(String contractId, String tenantId);

    boolean bootstrap(String contractId, String tenantId, String serviceName, Map<String, String> properties);
    
    SerializableDocumentDirectory getTenantServiceConfig(String contractId, String tenantId, String serviceName);
}
