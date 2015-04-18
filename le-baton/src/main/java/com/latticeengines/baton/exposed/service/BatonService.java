package com.latticeengines.baton.exposed.service;

import java.util.AbstractMap;
import java.util.List;
import java.util.Map;

import com.latticeengines.domain.exposed.camille.DocumentDirectory;
import com.latticeengines.domain.exposed.camille.bootstrap.BootstrapState;
import com.latticeengines.domain.exposed.camille.lifecycle.CustomerSpaceInfo;
import com.latticeengines.domain.exposed.camille.lifecycle.TenantInfo;

public interface BatonService {

    boolean createTenant(String contractId, String tenantId, String defaultSpaceId, CustomerSpaceInfo spaceInfo);

    boolean loadDirectory(String source, String destination);

    boolean loadDirectory(DocumentDirectory sourceDir, String destination);

    boolean bootstrap(String contractId, String tenantId, String spaceId, String serviceName,
            Map<String, String> properties);

    List<AbstractMap.SimpleEntry<String, TenantInfo>> getTenants(String contractId);

    boolean deleteTenant(String contractId, String tenantId);

    BootstrapState getTenantServiceBootstrapState(String contractId, String tenantId, String serviceName);

    DocumentDirectory getDefaultConfiguration(String serviceName);
}
