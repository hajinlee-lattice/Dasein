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
import com.latticeengines.domain.exposed.admin.LatticeProduct;
import com.latticeengines.domain.exposed.admin.SpaceConfiguration;
import com.latticeengines.domain.exposed.admin.TenantDocument;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.camille.Path;
import com.latticeengines.domain.exposed.camille.bootstrap.BootstrapState;
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
    public void setBootstrapState() throws Exception {
        zooKeeperManager.setBootstrapStateToMigrate(CUSTOMER);

        List<String> components = Arrays.asList(
                "PLS", "VisiDBDL", "VisiDBTemplate", "DLTemplate", "BardJams"
        );
        for (String component: components) {
            BootstrapState state = batonService.getTenantServiceBootstrapState(CUSTOMER, CUSTOMER, SPACE, component);
            Assert.assertEquals(state.state, BootstrapState.State.MIGRATED);
        }
    }
}
