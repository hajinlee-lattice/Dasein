package com.latticeengines.baton.exposed.service.impl;

import java.io.File;
import java.io.IOException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.ZooDefs;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.baton.exposed.service.BatonService;
import com.latticeengines.camille.exposed.Camille;
import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.config.bootstrap.CustomerSpaceServiceBootstrapManager;
import com.latticeengines.camille.exposed.config.bootstrap.ServiceBootstrapManager;
import com.latticeengines.camille.exposed.config.bootstrap.ServiceWarden;
import com.latticeengines.camille.exposed.featureflags.FeatureFlagClient;
import com.latticeengines.camille.exposed.lifecycle.ContractLifecycleManager;
import com.latticeengines.camille.exposed.lifecycle.SpaceLifecycleManager;
import com.latticeengines.camille.exposed.lifecycle.TenantLifecycleManager;
import com.latticeengines.camille.exposed.paths.FileSystemGetChildrenFunction;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.camille.exposed.paths.PathConstants;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.admin.LatticeFeatureFlag;
import com.latticeengines.domain.exposed.admin.LatticeProduct;
import com.latticeengines.domain.exposed.admin.SpaceConfiguration;
import com.latticeengines.domain.exposed.admin.TenantDocument;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.camille.DocumentDirectory;
import com.latticeengines.domain.exposed.camille.Path;
import com.latticeengines.domain.exposed.camille.bootstrap.BootstrapState;
import com.latticeengines.domain.exposed.camille.featureflags.FeatureFlagDefinition;
import com.latticeengines.domain.exposed.camille.featureflags.FeatureFlagValueMap;
import com.latticeengines.domain.exposed.camille.lifecycle.ContractInfo;
import com.latticeengines.domain.exposed.camille.lifecycle.ContractProperties;
import com.latticeengines.domain.exposed.camille.lifecycle.CustomerSpaceInfo;
import com.latticeengines.domain.exposed.camille.lifecycle.TenantInfo;
import com.latticeengines.domain.exposed.camille.lifecycle.TenantProperties;

public class BatonServiceImpl implements BatonService {

    private static final Logger log = LoggerFactory.getLogger(new Object() {
    }.getClass().getEnclosingClass());

    private static final int MAX_RETRY_TIMES = 3;

    @Override
    public boolean createTenant(String contractId, String tenantId, String defaultSpaceId,
            CustomerSpaceInfo spaceInfo) {
        ContractInfo contractInfo = new ContractInfo(new ContractProperties());
        TenantInfo tenantInfo = new TenantInfo(
                new TenantProperties(spaceInfo.properties.displayName, spaceInfo.properties.description));
        return createTenant(contractId, tenantId, defaultSpaceId, contractInfo, tenantInfo, spaceInfo);
    }

    @Override
    public boolean createTenant(String contractId, String tenantId, String defaultSpaceId, ContractInfo contractInfo,
            TenantInfo tenantInfo, CustomerSpaceInfo spaceInfo) {
        try {
            Camille camille = CamilleEnvironment.getCamille();
            Path contractsPath = PathBuilder.buildContractsPath(CamilleEnvironment.getPodId());
            if (!camille.exists(contractsPath)) {
                camille.create(contractsPath, ZooDefs.Ids.OPEN_ACL_UNSAFE);
            }
            if (!ContractLifecycleManager.exists(contractId)) {
                log.info(String.format("Creating contract %s", contractId));
                // XXX For now
                ContractLifecycleManager.create(contractId, contractInfo);
            }
            // Timestamp
            tenantInfo.properties.created = new DateTime().getMillis();
            tenantInfo.properties.lastModified = new DateTime().getMillis();

            TenantLifecycleManager.create(contractId, tenantId, tenantInfo, defaultSpaceId, spaceInfo);

            // Setup a dummy space configuration.
            // True space configuration will be handled in le-admin
            String spaceId = CustomerSpace.BACKWARDS_COMPATIBLE_SPACE_ID;
            Path spaceConfigPath = PathBuilder
                    .buildCustomerSpacePath(CamilleEnvironment.getPodId(), contractId, tenantId, spaceId)
                    .append(new Path("/" + PathConstants.SPACECONFIGURATION_NODE));
            if (!camille.exists(spaceConfigPath)) {
                setupSpaceConfiguration(contractId, tenantId, CustomerSpace.BACKWARDS_COMPATIBLE_SPACE_ID,
                        new SpaceConfiguration());
            }
        } catch (Exception e) {
            log.error("Error creating tenant", e);
            return false;
        }

        log.info(String.format("Successfully created tenant %s", tenantId));
        return true;
    }

    @Override
    public boolean loadDirectory(String source, String destination) {
        try {
            String rawPath = "";
            String podId = CamilleEnvironment.getPodId();

            // handle case where we want root pod directory
            if (destination.equals("")) {
                rawPath = String.format("/Pods/%s", podId.substring(0, podId.length()));
            } else {
                rawPath = String.format("/Pods/%s/%s", podId, destination);
            }

            Path parent = new Path(rawPath);
            File f = new File(source);
            DocumentDirectory docDir = new DocumentDirectory(new Path("/"), new FileSystemGetChildrenFunction(f));

            return loadDirectory(docDir, parent);

        } catch (IOException e) {
            log.error("Error converting source string to file", e);
            return false;
        }
    }

    @Override
    public boolean loadDirectory(DocumentDirectory sourceDir, Path absoluteRootPath) {
        String rawPath = "";
        try {
            Camille c = CamilleEnvironment.getCamille();
            // convert paths to relative to parent
            sourceDir.makePathsLocal();
            if (!c.exists(absoluteRootPath)) {
                c.create(absoluteRootPath, ZooDefs.Ids.OPEN_ACL_UNSAFE);
            }
            c.upsertDirectory(absoluteRootPath, sourceDir, ZooDefs.Ids.OPEN_ACL_UNSAFE);

        } catch (Exception e) {
            log.error("Error loading directory", e);
            return false;
        }

        log.info(String.format("Successfully loaded files into directory %s", rawPath));
        return true;
    }

    @Override
    public boolean bootstrap(String contractId, String tenantId, String spaceId, String serviceName,
            Map<String, String> properties) {
        CustomerSpace space = new CustomerSpace(contractId, tenantId, spaceId);
        if (properties == null) {
            properties = new HashMap<>();
        }
        try {
            log.info("Bootstrapping service " + serviceName + " in space " + space);
            ServiceWarden.commandBootstrap(serviceName, space, properties);
        } catch (Exception e) {
            log.error("Error commanding bootstrap for service " + serviceName + " and space " + space);
            return false;
        }
        return true;
    }

    @Override
    public Collection<TenantDocument> getTenants(String contractId) {
        if (contractId != null) {
            try {
                ContractInfo contractInfo = ContractLifecycleManager.getInfo(contractId);
                return getTenants(contractId, contractInfo);
            } catch (Exception e) {
                log.error(String.format("Error retrieving tenants in contract %s.", contractId), e);
                return null;
            }
        } else {
            return getTenants(null, null);
        }
    }

    @Override
    public boolean deleteTenant(String contractId, String tenantId) {
        try {
            CamilleEnvironment.getCamille();
            if (TenantLifecycleManager.exists(contractId, tenantId)) {
                TenantLifecycleManager.delete(contractId, tenantId);
                return true;
            } else {
                return false;
            }

        } catch (Exception e) {
            log.error("Error retrieving tenants", e);
            return false;
        }
    }

    @Override
    public boolean deleteContract(String contractId) {
        try {
            CamilleEnvironment.getCamille();
            if (ContractLifecycleManager.exists(contractId)) {
                ContractLifecycleManager.delete(contractId);
                return true;
            } else {
                return false;
            }

        } catch (Exception e) {
            log.error("Error retrieving tenants", e);
            return false;
        }
    }

    @Override
    public TenantDocument getTenant(String contractId, String tenantId) {
        TenantDocument doc = new TenantDocument();
        try {
            int failedTimes = 0;
            TenantInfo tenantInfo = null;
            while (true) {
                try {
                    tenantInfo = TenantLifecycleManager.getInfo(contractId, tenantId);
                } catch (NoNodeException e) {
                    if (failedTimes++ == MAX_RETRY_TIMES) {
                        log.error(String.format("Could not get the tenant info for tenant %s", tenantId));
                        throw e;
                    }
                }
                if (tenantInfo != null) {
                    break;
                }
                Thread.sleep(500);
            }

            if (tenantInfo == null) {
                return null;
            }
            doc.setTenantInfo(tenantInfo);

            try {
                ContractInfo contractInfo = ContractLifecycleManager.getInfo(contractId);
                doc.setContractInfo(contractInfo);
            } catch (Exception e) {
                log.error(String.format("Could not get the info of the default space for tenant %s", tenantId));
            }

            try {
                DocumentDirectory spaceConfigDir = getSpaceConfiguration(contractId, tenantId);
                SpaceConfiguration spaceConfig = new SpaceConfiguration(spaceConfigDir);
                doc.setSpaceConfig(spaceConfig);
            } catch (Exception e) {
                log.error(String.format("Could not get the space configuration directory for tenant %s", tenantId));
            }

            String spaceId = CustomerSpace.BACKWARDS_COMPATIBLE_SPACE_ID;

            try {
                CustomerSpaceInfo spaceInfo = SpaceLifecycleManager.getInfo(contractId, tenantId, spaceId);
                doc.setSpaceInfo(spaceInfo);
            } catch (Exception e) {
                log.error(String.format("Could not get the info of the default space for tenant %s", tenantId));
            }

            CustomerSpace space = new CustomerSpace(contractId, tenantId, spaceId);
            doc.setSpace(space);
        } catch (Exception e) {
            log.error(String.format("Error retrieving tenant %s in %s", tenantId, contractId), e);
        }
        return doc;
    }

    @Override
    public boolean discardService(String serviceName) {
        try {
            Camille camille = CamilleEnvironment.getCamille();
            String podId = CamilleEnvironment.getPodId();
            Path serviceRootPath = PathBuilder.buildServicePath(podId, serviceName);
            if (camille.exists(serviceRootPath)) {
                camille.delete(serviceRootPath);
                return true;
            } else {
                return false;
            }

        } catch (Exception e) {
            log.error("Error discarding service " + serviceName, e);
            return false;
        }
    }

    @Override
    public Set<String> getRegisteredServices() {
        try {
            return ServiceBootstrapManager.getRegisteredBootstrappers();
        } catch (Exception e) {
            log.error("Error getting all registered services.", e);
            return null;
        }
    }

    @Override
    public BootstrapState getTenantServiceBootstrapState(String contractId, String tenantId, String serviceName) {
        return getTenantServiceBootstrapState(contractId, tenantId, CustomerSpace.BACKWARDS_COMPATIBLE_SPACE_ID,
                serviceName);
    }

    @Override
    public BootstrapState getTenantServiceBootstrapState(String contractId, String tenantId, String spaceId,
            String serviceName) {
        CustomerSpace customerSpace = new CustomerSpace(contractId, tenantId, spaceId);
        try {
            return CustomerSpaceServiceBootstrapManager.getBootstrapState(serviceName, customerSpace);
        } catch (Exception e) {
            log.error("Error retrieving tenant service state", e);
            return null;
        }
    }

    @Override
    public DocumentDirectory getDefaultConfiguration(String serviceName) {
        try {
            Camille camille = CamilleEnvironment.getCamille();
            String podId = CamilleEnvironment.getPodId();
            Path defaultConfigPath = PathBuilder.buildServiceDefaultConfigPath(podId, serviceName);
            return camille.getDirectory(defaultConfigPath);
        } catch (Exception e) {
            log.error("Error retrieving default config for service " + serviceName, e);
            return null;
        }
    }

    @Override
    public DocumentDirectory getConfigurationSchema(String serviceName) {
        try {
            Camille camille = CamilleEnvironment.getCamille();
            String podId = CamilleEnvironment.getPodId();
            Path metadataPath = PathBuilder.buildServiceConfigSchemaPath(podId, serviceName);
            return camille.getDirectory(metadataPath);
        } catch (Exception e) {
            log.error("Error retrieving configuration schema for service " + serviceName, e);
            return null;
        }
    }

    private DocumentDirectory getSpaceConfiguration(String contractId, String tenantId) {
        Path spaceConfigPath = PathBuilder.buildCustomerSpacePath(CamilleEnvironment.getPodId(), contractId, tenantId,
                CustomerSpace.BACKWARDS_COMPATIBLE_SPACE_ID).append(new Path("/SpaceConfiguration"));
        return CamilleEnvironment.getCamille().getDirectory(spaceConfigPath);
    }

    public Collection<TenantDocument> getTenants(String contractId, ContractInfo contractInfo) {
        List<TenantDocument> tenantDocs = new ArrayList<>();
        if (contractId != null) {
            try {
                List<AbstractMap.SimpleEntry<String, TenantInfo>> tenantEntries = TenantLifecycleManager
                        .getAll(contractId);
                if (contractInfo == null) {
                    contractInfo = ContractLifecycleManager.getInfo(contractId);
                }
                return constructTenantDocsWithDefaultSpaceId(tenantEntries, contractId, contractInfo);
            } catch (Exception e) {
                log.error(String.format("Error retrieving tenants in contract %s.", contractId), e);
            }
        } else {
            List<AbstractMap.SimpleEntry<String, ContractInfo>> contracts = new ArrayList<>();
            try {
                contracts = ContractLifecycleManager.getAll();
            } catch (Exception e) {
                log.error("Error retrieving all contracts.", e);
            }
            if (!contracts.isEmpty()) {
                for (Map.Entry<String, ContractInfo> contract : contracts) {
                    try {
                        tenantDocs.addAll(getTenants(contract.getKey(), contract.getValue()));
                    } catch (Exception e) {
                        log.error(String.format("Error retrieving tenants in contract %s.", contract.getKey()), e);
                    }
                }
            }
        }

        return tenantDocs;
    }

    private List<TenantDocument> constructTenantDocsWithDefaultSpaceId(
            List<AbstractMap.SimpleEntry<String, TenantInfo>> tenantEntries, String contractId,
            ContractInfo contractInfo) {
        if (tenantEntries == null)
            return null;

        List<TenantDocument> docs = new ArrayList<>();
        for (Map.Entry<String, TenantInfo> tenantEntry : tenantEntries) {
            String tenantId = tenantEntry.getKey();
            String spaceId = CustomerSpace.BACKWARDS_COMPATIBLE_SPACE_ID;

            try {
                TenantDocument doc = new TenantDocument();

                DocumentDirectory spaceConfigDir = getSpaceConfiguration(contractId, tenantId);
                SpaceConfiguration spaceConfig = null;
                try {
                    spaceConfig = new SpaceConfiguration(spaceConfigDir);
                } catch (Exception e) {
                    // ignore
                }

                CustomerSpace space = new CustomerSpace(contractId, tenantId, spaceId);
                CustomerSpaceInfo spaceInfo = SpaceLifecycleManager.getInfo(contractId, tenantId, spaceId);

                doc.setSpace(space);
                doc.setSpaceConfig(spaceConfig);
                doc.setTenantInfo(tenantEntry.getValue());
                doc.setContractInfo(contractInfo);
                doc.setSpaceInfo(spaceInfo);
                docs.add(doc);
            } catch (Exception e) {
                log.error(String.format("Error constructing tenant document for contract %s, tenant %s, and space %s.",
                        contractId, tenantId, spaceId), e);
            }
        }

        return docs;
    }

    @Override
    public boolean setupSpaceConfiguration(String contractId, String tenantId, String spaceId,
            SpaceConfiguration spaceConfig) {
        return setupSpaceConfiguration(contractId, tenantId, spaceId, spaceConfig.toDocumentDirectory());
    }

    private boolean setupSpaceConfiguration(String contractId, String tenantId, String spaceId,
            DocumentDirectory spaceConfig) {
        Path spaceConfigPath = PathBuilder
                .buildCustomerSpacePath(CamilleEnvironment.getPodId(), contractId, tenantId, spaceId)
                .append(new Path("/" + PathConstants.SPACECONFIGURATION_NODE));
        return loadDirectory(spaceConfig, spaceConfigPath);
    }

    public boolean hasProduct(CustomerSpace customerSpace, LatticeProduct product) {
        String contractId = customerSpace.getContractId();
        String tenantId = customerSpace.getTenantId();
        Path productsPath = PathBuilder.buildCustomerSpacePath(CamilleEnvironment.getPodId(), contractId, tenantId,
                CustomerSpace.BACKWARDS_COMPATIBLE_SPACE_ID).append("SpaceConfiguration").append("Products");
        try {
            String data = CamilleEnvironment.getCamille().get(productsPath).getData();
            List<String> productStrs = JsonUtils.convertList(JsonUtils.deserialize(data, List.class), String.class);
            return productStrs != null && productStrs.stream().anyMatch(product.getName()::equals);
        } catch (Exception e) {
            throw new RuntimeException("Failed to get products for customer " + tenantId, e);
        }
    }

    @SuppressWarnings("deprecation")
    public boolean isEnabled(CustomerSpace customerSpace, LatticeFeatureFlag flag) {
        return canHaveFlag(customerSpace, flag) && FeatureFlagClient.isEnabled(customerSpace, flag.getName());
    }

    @SuppressWarnings("deprecation")
    public FeatureFlagValueMap getFeatureFlags(CustomerSpace customerSpace) {
        FeatureFlagValueMap valueMapInCamille = FeatureFlagClient.getFlags(customerSpace);
        FeatureFlagValueMap valueMap = new FeatureFlagValueMap(valueMapInCamille);
        Arrays.stream(LatticeFeatureFlag.values()).forEach(flag -> {
            if (canHaveFlag(customerSpace, flag)) {
                if (flag.isDeprecated() || !valueMapInCamille.containsKey(flag.getName())) {
                    FeatureFlagDefinition definition = FeatureFlagClient.getDefinition(flag.getName());
                    valueMap.put(flag.getName(), definition.getDefaultValue());
                }
            }
        });
        return valueMap;
    }

    private boolean canHaveFlag(CustomerSpace customerSpace, LatticeFeatureFlag flag) {
        FeatureFlagDefinition definition = FeatureFlagClient.getDefinition(flag.getName());
        return hasAtLeastOneProduct(customerSpace, definition.getAvailableProducts());
    }

    private boolean hasAtLeastOneProduct(CustomerSpace customerSpace, Collection<LatticeProduct> products) {
        return products.stream().anyMatch(product -> hasProduct(customerSpace, product));
    }

}