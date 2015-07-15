package com.latticeengines.upgrade.zk;

import java.util.Arrays;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.baton.exposed.service.BatonService;
import com.latticeengines.camille.exposed.Camille;
import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.common.exposed.util.CipherUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.admin.CRMTopology;
import com.latticeengines.domain.exposed.admin.LatticeProduct;
import com.latticeengines.domain.exposed.admin.SpaceConfiguration;
import com.latticeengines.domain.exposed.admin.TenantDocument;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.camille.Document;
import com.latticeengines.domain.exposed.camille.Path;
import com.latticeengines.domain.exposed.camille.bootstrap.BootstrapState;
import com.latticeengines.domain.exposed.pls.CrmCredential;
import com.latticeengines.upgrade.functionalframework.UpgradeFunctionalTestNGBase;

public class ZooKeeperManagerTestNG extends UpgradeFunctionalTestNGBase {

    private static final String SPACE = CustomerSpace.BACKWARDS_COMPATIBLE_SPACE_ID;

    @Autowired
    private ZooKeeperManager zooKeeperManager;

    @Autowired
    private BatonService batonService;

    private String podId;
    private Camille camille;

    @BeforeClass(groups = "functional")
    public void setup() throws Exception {
        podId = CamilleEnvironment.getPodId();
        camille = CamilleEnvironment.getCamille();
        teardown();
    }

    @AfterClass(groups = "functional")
    public void teardown() throws Exception {
        Path contractRoot = PathBuilder.buildContractPath(podId, CUSTOMER);
        if (camille.exists(contractRoot)) camille.delete(contractRoot);
    }

    @Test(groups = "functional")
    public void registerTenant() throws Exception {
        zooKeeperManager.registerTenantIfNotExist(CUSTOMER);
        Path spacePath = PathBuilder.buildCustomerSpacePath(podId, CUSTOMER, CUSTOMER, SPACE);
        Assert.assertTrue(camille.exists(spacePath),
                String.format("The space %s does not exist in ZK.", CustomerSpace.parse(CUSTOMER)));

        // idempotent
        zooKeeperManager.registerTenantIfNotExist(CUSTOMER);
        spacePath = PathBuilder.buildCustomerSpacePath(podId, CUSTOMER, CUSTOMER, SPACE);
        Assert.assertTrue(camille.exists(spacePath),
                String.format("Idempotent test failed: space %s does not exist in ZK.", CustomerSpace.parse(CUSTOMER)));
    }

    @Test(groups = "functional", dependsOnMethods = "registerTenant")
    public void uploadSpaceConfiguration() throws Exception {
        SpaceConfiguration spaceConfiguration = new SpaceConfiguration();
        spaceConfiguration.setDlAddress(DL_URL);
        spaceConfiguration.setTopology(TOPOLOGY);
        spaceConfiguration.setProduct(LatticeProduct.LPA);

        zooKeeperManager.uploadSpaceConfiguration(CUSTOMER, spaceConfiguration);

        TenantDocument doc = batonService.getTenant(CUSTOMER, CUSTOMER);
        SpaceConfiguration newSpaceConfig = doc.getSpaceConfig();

        Assert.assertEquals(newSpaceConfig.getDlAddress(), DL_URL);
        Assert.assertEquals(newSpaceConfig.getTopology(), TOPOLOGY);
        Assert.assertEquals(newSpaceConfig.getProduct(), LatticeProduct.LPA);
    }

    @Test(groups = "functional", dependsOnMethods = "registerTenant")
    public void uploadCredentials() throws Exception {
        zooKeeperManager.uploadCrmCredentials(CUSTOMER, DL_URL, CRMTopology.MARKETO);

        checkCrmCredential("marketo", getCredential("marketo", CUSTOMER));
        checkCrmCredential("marketo", getCredential("marketo", TUPLE_ID));

        checkCrmCredential("sfdc", getCredential("sfdc", CUSTOMER));
        checkCrmCredential("sfdc", getCredential("sfdc", TUPLE_ID));

        removeCredentials("marketo", CUSTOMER);
        removeCredentials("sfdc", CUSTOMER);
    }

    @Test(groups = "functional", dependsOnMethods = "registerTenant")
    public void setBootstrapState() throws Exception {
        zooKeeperManager.setBootstrapStateToMigrate(CUSTOMER);

        List<String> components = Arrays.asList("PLS", "VisiDBDL", "VisiDBTemplate", "DLTemplate", "BardJams");
        for (String component: components) {
            BootstrapState state = batonService.getTenantServiceBootstrapState(CUSTOMER, CUSTOMER, SPACE, component);
            Assert.assertEquals(state.state, BootstrapState.State.MIGRATED);
        }
    }

    private CrmCredential getCredential(String crmType, String tenantId) throws Exception {
        CustomerSpace customerSpace = CustomerSpace.parse(tenantId);

        Path docPath = PathBuilder.buildCustomerSpacePath(CamilleEnvironment.getPodId(),
                customerSpace.getContractId(), customerSpace.getTenantId(), customerSpace.getSpaceId());
        docPath = addExtraPath(crmType, docPath, true);

        Camille camille = CamilleEnvironment.getCamille();
        Document doc = camille.get(docPath);
        CrmCredential crmCredential = JsonUtils.deserialize(doc.getData(), CrmCredential.class);
        crmCredential.setPassword(CipherUtils.decrypt(crmCredential.getPassword()));

        return crmCredential;
    }

    private void removeCredentials(String crmType, String tenantId) throws Exception {
        CustomerSpace customerSpace = CustomerSpace.parse(tenantId);

        Path docPath = PathBuilder.buildCustomerSpacePath(CamilleEnvironment.getPodId(),
                customerSpace.getContractId(), customerSpace.getTenantId(), customerSpace.getSpaceId());
        docPath = addExtraPath(crmType, docPath, true);

        Camille camille = CamilleEnvironment.getCamille();
        if (camille.exists(docPath)) camille.delete(docPath);
    }

    private Path addExtraPath(String crmType, Path docPath, Boolean isProduction) {
        docPath = docPath.append(crmType);
        if (crmType.equalsIgnoreCase("sfdc")) {
            docPath = docPath.append(isProduction ? "Production" : "Sandbox");
        }
        return docPath;
    }

    private void checkCrmCredential(String crmType, CrmCredential crmCredential) {
        Assert.assertNotNull(crmCredential, "Should be able to find credential in ZK.");
        if (crmType.equalsIgnoreCase("marketo")) {
            Assert.assertNotNull(crmCredential.getUrl(), "Marketo credential should have url.");
        }
        if (crmType.equalsIgnoreCase("eloqua")) {
            Assert.assertNotNull(crmCredential.getCompany(), "Eloqua credential should have company.");
        }
        if (crmType.equalsIgnoreCase("sfdc")) {
            Assert.assertNotNull(crmCredential.getSecurityToken(), "SFDC credential should have security token.");
        }
    }
}
