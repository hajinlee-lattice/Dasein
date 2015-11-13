package com.latticeengines.admin.functionalframework;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.client.ClientHttpRequestInterceptor;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;

import com.latticeengines.camille.exposed.Camille;
import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.lifecycle.ContractLifecycleManager;
import com.latticeengines.camille.exposed.lifecycle.TenantLifecycleManager;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.domain.exposed.admin.TenantDocument;
import com.latticeengines.domain.exposed.camille.Document;
import com.latticeengines.domain.exposed.camille.Path;
import com.latticeengines.domain.exposed.camille.lifecycle.ContractInfo;
import com.latticeengines.security.exposed.Constants;

public class AdminFunctionalTestNGBase extends AdminAbstractTestNGBase {

    private static final Log log = LogFactory.getLog(AdminFunctionalTestNGBase.class);
    private static boolean ZKIsClean = false;

    @Value("${admin.test.functional.api}")
    protected String hostPort;

    @Override
    protected String getRestHostPort() {
        return hostPort.endsWith("/") ? hostPort.substring(0, hostPort.length() - 1) : hostPort;
    }

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
                            // ignore
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
                                    log.warn("Found a bad tenant: " + contractId + "-" + tenantId + ". Deleting it ...");
                                    try {
                                        TenantLifecycleManager.delete(contractId, tenantId);
                                    } catch (Exception e2) {
                                        // ignore
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
        cleanupZK();
        loginAD();

        String podId = CamilleEnvironment.getPodId();
        Assert.assertNotNull(podId);

        try {
            deleteTenant(TestContractId, TestTenantId);
        } catch (Exception e) {
            // ignore
        }
        createTenant(TestContractId, TestTenantId);

        // setup magic rest template
        addMagicAuthHeader.setAuthValue(Constants.INTERNAL_SERVICE_HEADERVALUE);
        magicRestTemplate.setInterceptors(Arrays.asList(new ClientHttpRequestInterceptor[] { addMagicAuthHeader }));
    }

    @AfterClass(groups = { "functional" })
    public void tearDown() throws Exception {
        try {
            deleteTenant(TestContractId, TestTenantId);
        } catch (Exception e) {
            // ignore
        }
    }

}
