package com.latticeengines.baton.exposed.service;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

import com.latticeengines.domain.exposed.admin.SpaceConfiguration;
import com.latticeengines.domain.exposed.admin.TenantDocument;
import com.latticeengines.domain.exposed.camille.DocumentDirectory;
import com.latticeengines.domain.exposed.camille.Path;
import com.latticeengines.domain.exposed.camille.bootstrap.BootstrapState;
import com.latticeengines.domain.exposed.camille.lifecycle.ContractInfo;
import com.latticeengines.domain.exposed.camille.lifecycle.CustomerSpaceInfo;
import com.latticeengines.domain.exposed.camille.lifecycle.TenantInfo;

public interface BatonService {

    boolean createTenant(String contractId, String tenantId, String defaultSpaceId, CustomerSpaceInfo spaceInfo);

    boolean createTenant(String contractId, String tenantId, String defaultSpaceId,
                         ContractInfo contractInfo, TenantInfo tenantInfo, CustomerSpaceInfo spaceInfo);

    TenantDocument getTenant(String contractId, String tenantId);

    boolean loadDirectory(String source, String destination);

    boolean loadDirectory(DocumentDirectory sourceDir, Path absoluteRootPath);

    boolean bootstrap(String contractId, String tenantId, String spaceId, String serviceName,
            Map<String, String> properties);

    Collection<TenantDocument> getTenants(String contractId);

    boolean deleteTenant(String contractId, String tenantId);

    boolean discardService(String serviceName);

    Set<String> getRegisteredServices();

    BootstrapState getTenantServiceBootstrapState(String contractId, String tenantId, String serviceName);

    BootstrapState getTenantServiceBootstrapState(String contractId, String tenantId, String spaceId, String serviceName);

    DocumentDirectory getDefaultConfiguration(String serviceName);

    DocumentDirectory getConfigurationSchema(String serviceName);

    boolean setupSpaceConfiguration(String contractId, String tenantId, String spaceId,
                                    SpaceConfiguration spaceConfig);
}
