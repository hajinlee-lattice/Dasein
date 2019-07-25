package com.latticeengines.baton.exposed.service;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

import org.apache.curator.framework.recipes.cache.TreeCache;

import com.latticeengines.domain.exposed.admin.LatticeFeatureFlag;
import com.latticeengines.domain.exposed.admin.LatticeProduct;
import com.latticeengines.domain.exposed.admin.SpaceConfiguration;
import com.latticeengines.domain.exposed.admin.TenantDocument;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.camille.DocumentDirectory;
import com.latticeengines.domain.exposed.camille.Path;
import com.latticeengines.domain.exposed.camille.bootstrap.BootstrapState;
import com.latticeengines.domain.exposed.camille.featureflags.FeatureFlagValueMap;
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

    Collection<TenantDocument> getTenantsInCache(String contractId);

    boolean deleteTenant(String contractId, String tenantId);

    boolean deleteContract(String contractId);

    boolean discardService(String serviceName);

    Set<String> getRegisteredServices();

    BootstrapState getTenantServiceBootstrapState(String contractId, String tenantId, String serviceName);

    BootstrapState getTenantServiceBootstrapStateInCache(String contractId, String tenantId, String serviceName,
            TreeCache cache);

    BootstrapState getTenantServiceBootstrapState(String contractId, String tenantId, String spaceId, String serviceName);

    BootstrapState getTenantServiceBootstrapStateInCache(String contractId, String tenantId, String spaceId,
            String serviceName, TreeCache cache);

    DocumentDirectory getDefaultConfiguration(String serviceName);

    DocumentDirectory getConfigurationSchema(String serviceName);

    boolean setupSpaceConfiguration(String contractId, String tenantId, String spaceId,
                                    SpaceConfiguration spaceConfig);

    boolean isEnabled(CustomerSpace customerSpace, LatticeFeatureFlag flag);

    boolean isEntityMatchEnabled(CustomerSpace customerSpace);

    void setFeatureFlag(CustomerSpace customerSpace, LatticeFeatureFlag flag, boolean value);

    boolean hasProduct(CustomerSpace customerSpace, LatticeProduct product);

    FeatureFlagValueMap getFeatureFlags(CustomerSpace customerSpace);

}
