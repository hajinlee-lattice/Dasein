package com.latticeengines.baton.exposed.service;

import java.util.AbstractMap;
import java.util.List;

import com.latticeengines.domain.exposed.camille.lifecycle.CustomerSpaceInfo;
import com.latticeengines.domain.exposed.camille.lifecycle.TenantInfo;

public interface BatonService {

    void createTenant(String contractId, String tenantId, String defaultSpaceId, CustomerSpaceInfo spaceInfo);

    void loadDirectory(String source, String destination);

    void bootstrap(String contractId, String tenantId, String spaceId, String serviceName);

    List<AbstractMap.SimpleEntry<String, TenantInfo>> getTenants(String contractId);

    Boolean deleteTenant(String contractId, String tenantId);
}
