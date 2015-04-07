package com.latticeengines.admin.service;

import java.util.AbstractMap;
import java.util.List;
import java.util.Set;

import com.latticeengines.domain.exposed.camille.lifecycle.TenantInfo;

public interface TenantService {

    List<AbstractMap.SimpleEntry<String, TenantInfo>> getTenants(String contractId);
    
    Boolean deleteTenant(String contractId, String tenantId);
    
    Set<String> getRegisteredServices();

}
