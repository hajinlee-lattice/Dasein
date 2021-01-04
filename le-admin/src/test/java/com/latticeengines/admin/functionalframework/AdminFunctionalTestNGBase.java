package com.latticeengines.admin.functionalframework;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;

import com.latticeengines.camille.exposed.Camille;
import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.lifecycle.ContractLifecycleManager;
import com.latticeengines.camille.exposed.lifecycle.TenantLifecycleManager;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.domain.exposed.admin.SerializableDocumentDirectory;
import com.latticeengines.domain.exposed.admin.SpaceConfiguration;
import com.latticeengines.domain.exposed.admin.TenantDocument;
import com.latticeengines.domain.exposed.admin.TenantRegistration;
import com.latticeengines.domain.exposed.camille.Document;
import com.latticeengines.domain.exposed.camille.DocumentDirectory;
import com.latticeengines.domain.exposed.camille.Path;
import com.latticeengines.domain.exposed.camille.bootstrap.BootstrapState;
import com.latticeengines.domain.exposed.camille.lifecycle.ContractInfo;
import com.latticeengines.domain.exposed.camille.lifecycle.ContractProperties;
import com.latticeengines.domain.exposed.camille.lifecycle.CustomerSpaceInfo;
import com.latticeengines.domain.exposed.camille.lifecycle.CustomerSpaceProperties;
import com.latticeengines.domain.exposed.camille.lifecycle.TenantInfo;
import com.latticeengines.domain.exposed.camille.lifecycle.TenantProperties;

public class AdminFunctionalTestNGBase extends AdminAbstractTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(AdminFunctionalTestNGBase.class);
    private static boolean ZKIsClean = false;

    @Value("${admin.test.functional.cleanupZK}")
    protected Boolean cleanZK;

    protected void cleanupZK() {
        if (ZKIsClean)
            return;

        log.info("Checking the sanity of contracts and tenants in ZK.");

        boolean ZKHasIssues = false;

        try {
            for (TenantDocument tenantDocument : batonService.getTenants(null)) {
                if (tenantDocument.getContractInfo() == null || tenantDocument.getTenantInfo() == null
                        || tenantDocument.getSpaceInfo() == null || tenantDocument.getSpaceConfig() == null) {
                    ZKHasIssues = true;
                    break;
                }
            }
        } catch (Exception e) {
            ZKHasIssues = true;
        }

        if (ZKHasIssues) {
            log.info("Cleaning up bad contracts and tenants in ZK");
            Camille camille = CamilleEnvironment.getCamille();
            String podId = CamilleEnvironment.getPodId();
            boolean contractsExist = false;
            try {
                contractsExist = camille.exists(PathBuilder.buildContractsPath(podId));
            } catch (Exception e) {
                log.warn("Getting Contracts node error.");
            }

            if (contractsExist) {
                List<AbstractMap.SimpleEntry<Document, Path>> contractDocs = new ArrayList<>();
                try {
                    contractDocs = camille.getChildren(PathBuilder.buildContractsPath(podId));
                } catch (Exception e) {
                    log.warn("Getting Contract Documents error.");
                }

                for (AbstractMap.SimpleEntry<Document, Path> entry : contractDocs) {
                    String contractId = entry.getValue().getSuffix();
                    ContractInfo contractInfo = null;
                    try {
                        contractInfo = ContractLifecycleManager.getInfo(contractId);
                    } catch (Exception e) {
                        log.warn("Found a bad contract: " + contractId + ". Deleting it ...");
                        try {
                            ContractLifecycleManager.delete(contractId);
                        } catch (Exception e2) {
                            log.debug("Contract {} has already been removed.", contractId);
                        }
                    }

                    if (contractInfo != null) {
                        boolean tenantsExist = false;
                        try {
                            tenantsExist = camille.exists(PathBuilder.buildTenantsPath(podId, contractId));
                        } catch (Exception e) {
                            log.warn(String.format("Getting Tenants node for contract %s error.", contractId));
                        }
                        if (tenantsExist) {
                            List<AbstractMap.SimpleEntry<Document, Path>> tenantDocs = new ArrayList<>();
                            try {
                                tenantDocs = camille.getChildren(PathBuilder.buildTenantsPath(podId, contractId));
                            } catch (Exception e) {
                                log.warn(String.format("Getting Tenant Documents for contract %s error.", contractId));
                            }

                            for (AbstractMap.SimpleEntry<Document, Path> tenantEntry : tenantDocs) {
                                String tenantId = tenantEntry.getValue().getSuffix();

                                try {
                                    TenantDocument tenantDocument = batonService.getTenant(contractId, tenantId);
                                    if (tenantDocument.getContractInfo() == null
                                            || tenantDocument.getTenantInfo() == null
                                            || tenantDocument.getSpaceInfo() == null
                                            || tenantDocument.getSpaceConfig() == null) {
                                        throw new Exception("Tenant: " + contractId + "-" + tenantId
                                                + " does not have a fully valid TenantDocument.");
                                    }
                                } catch (Exception e) {
                                    log.warn(
                                            "Found a bad tenant: " + contractId + "-" + tenantId + ". Deleting it ...");
                                    try {
                                        TenantLifecycleManager.delete(contractId, tenantId);
                                    } catch (Exception e2) {
                                        log.debug("Tenant {} in contract {} has already been removed.", tenantId,
                                                contractId);
                                    }
                                }

                            }
                        }
                    }
                }

            }
        }

        ZKHasIssues = false;
        try {
            for (TenantDocument tenantDocument : batonService.getTenants(null)) {
                Assert.assertNotNull(tenantDocument.getContractInfo());
                Assert.assertNotNull(tenantDocument.getTenantInfo());
                Assert.assertNotNull(tenantDocument.getSpaceInfo());
                Assert.assertNotNull(tenantDocument.getSpaceConfig());
            }
        } catch (Exception e) {
            ZKHasIssues = true;
        }

        Assert.assertFalse(ZKHasIssues);

        ZKIsClean = true;
    }

    @BeforeClass(groups = { "functional" })
    public void setup() throws Exception {
        if (cleanZK) {
            cleanupZK();
        }
        String podId = CamilleEnvironment.getPodId();
        Assert.assertNotNull(podId);

        Level originalLevel = LogManager.getLogger(TenantLifecycleManager.class).getLevel();
        LogManager.getLogger(TenantLifecycleManager.class).setLevel(Level.DEBUG);

        try {
            deleteTenant(TestContractId, TestTenantId);
        } catch (Exception ignore) {
            // tenant does not exist
        }
        createTenant(TestContractId, TestTenantId);
        LogManager.getLogger(TenantLifecycleManager.class).setLevel(originalLevel);
    }

    @Override
    protected void createTenant(String contractId, String tenantId) {
        CustomerSpaceProperties props = new CustomerSpaceProperties();
        props.description = String.format("Test tenant for contract id %s and tenant id %s", contractId, tenantId);
        props.displayName = tenantId + ": Tenant for testing";
        CustomerSpaceInfo spaceInfo = new CustomerSpaceInfo(props, "");

        ContractInfo contractInfo = new ContractInfo(new ContractProperties());
        TenantInfo tenantInfo = new TenantInfo(
                new TenantProperties(spaceInfo.properties.displayName, spaceInfo.properties.description));

        SpaceConfiguration spaceConfig = tenantService.getDefaultSpaceConfig();

        TenantRegistration reg = new TenantRegistration();
        reg.setSpaceInfo(spaceInfo);
        reg.setTenantInfo(tenantInfo);
        reg.setContractInfo(contractInfo);
        reg.setSpaceConfig(spaceConfig);

        tenantService.createTenant(contractId, tenantId, reg, ADTesterUsername, null, null);
    }

    @Override
    protected void bootstrap(String contractId, String tenantId, String serviceName) {
        DocumentDirectory configDir = batonService.getDefaultConfiguration(serviceName);
        SerializableDocumentDirectory sDir = new SerializableDocumentDirectory(configDir);
        Map<String, String> bootstrapProperties = sDir.flatten();
        tenantService.bootstrap(contractId, tenantId, serviceName, bootstrapProperties);
    }

    @Override
    protected String getRestHostPort() {
        throw new UnsupportedOperationException();
    }

    @Override
    protected void deleteTenant(String contractId, String tenantId) {
        tenantService.deleteTenant(ADTesterUsername, contractId, tenantId, true);
    }

    @AfterClass(groups = { "functional" })
    public void tearDown() {
        try {
            deleteTenant(TestContractId, TestTenantId);
        } catch (Exception ignore) {
            // tenant does not exist
        }
    }

    @Override
    protected BootstrapState waitUntilStateIsNotInitial(String contractId, String tenantId, String serviceName) {
        return waitUntilStateIsNotInitial(contractId, tenantId, serviceName, 100);
    }

    @Override
    protected BootstrapState waitUntilStateIsNotInitial(String contractId, String tenantId, String serviceName,
            int numOfRetries) {
        BootstrapState state;
        do {
            numOfRetries--;
            try {
                Thread.sleep(2000L);
            } catch (InterruptedException e) {
                throw new RuntimeException("Waiting for component state update interrupted", e);
            }
            state = tenantService.getTenantServiceState(contractId, tenantId, serviceName);
        } while (state != null && state.state.equals(BootstrapState.State.INITIAL) && numOfRetries > 0);
        return state;
    }

    @Override
    protected void waitForTenantInstallation(String tenantId, String contractId) {
        long timeout = System.currentTimeMillis() + TimeUnit.MINUTES.toMillis(10L);
        BootstrapState state = BootstrapState.createInitialState();
        while (!BootstrapState.State.OK.equals(state.state) && !BootstrapState.State.ERROR.equals(state.state)
                && System.currentTimeMillis() <= timeout) {
            try {
                TenantDocument tenantDoc = tenantService.getTenant(contractId, tenantId);
                BootstrapState newState = tenantDoc.getBootstrapState();
                log.info("BootstrapState from tenant console: " + (newState == null ? null : newState.state));
                state = newState == null ? state : newState;
                if (BootstrapState.State.OK.equals(state.state) || BootstrapState.State.ERROR.equals(state.state)) {
                    return;
                }
            } catch (Exception e) {
                throw new RuntimeException("Failed to query tenant installation state", e);
            } finally {
                try {
                    Thread.sleep(5000L);
                } catch (InterruptedException e) {
                    log.error(e.getMessage(), e);
                }
            }
        }
    }
}
