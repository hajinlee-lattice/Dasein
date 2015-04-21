package com.latticeengines.admin.service;

import java.util.AbstractMap;
import java.util.List;
import java.util.Map;

import com.latticeengines.domain.exposed.admin.SerializableDocumentDirectory;
import com.latticeengines.domain.exposed.admin.TenantRegistration;
import com.latticeengines.domain.exposed.camille.bootstrap.BootstrapState;
import com.latticeengines.domain.exposed.camille.lifecycle.CustomerSpaceInfo;
import com.latticeengines.domain.exposed.camille.lifecycle.TenantInfo;

public interface TenantService {
    
    Boolean createTenant(String contractId, String tenantId, TenantRegistration tenantRegistration);

    List<AbstractMap.SimpleEntry<String, TenantInfo>> getTenants(String contractId);
    
    Boolean deleteTenant(String contractId, String tenantId);

    BootstrapState getTenantServiceState(String contractId, String tenantId, String serviceName);
    
    Boolean bootstrap(String contractId, String tenantId, String serviceName, Map<String, String> properties);
    
    SerializableDocumentDirectory getTenantServiceConfig(String contractId, String tenantId, String serviceName);
}
