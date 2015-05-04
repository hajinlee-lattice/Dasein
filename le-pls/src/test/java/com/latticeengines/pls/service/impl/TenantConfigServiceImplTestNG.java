package com.latticeengines.pls.service.impl;

import org.springframework.beans.factory.annotation.Autowired;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.camille.exposed.Camille;
import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.lifecycle.SpaceLifecycleManager;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.domain.exposed.camille.Path;
import com.latticeengines.domain.exposed.camille.lifecycle.CustomerSpaceInfo;
import com.latticeengines.domain.exposed.camille.lifecycle.CustomerSpaceProperties;
import com.latticeengines.pls.functionalframework.PlsFunctionalTestNGBase;
import com.latticeengines.pls.service.TenantConfigService;

public class TenantConfigServiceImplTestNG extends PlsFunctionalTestNGBase {

    @Autowired
    private TenantConfigService configService;

    @BeforeClass(groups = { "functional" })
    public void setup() throws Exception {
        Camille camille = CamilleEnvironment.getCamille();
        Path path = PathBuilder.buildCustomerSpacePath(CamilleEnvironment.getPodId(), "contractId", "tenantId",
                "spaceId");
        try {
            camille.delete(path);
        } catch (Exception ex) {
        }
        CustomerSpaceProperties properties = new CustomerSpaceProperties();
        properties.topology = "sfdc";
        CustomerSpaceInfo spaceInfo = new CustomerSpaceInfo(properties, "");

        SpaceLifecycleManager.create("contractId", "tenantId", "spaceId", spaceInfo);
    }

    @AfterClass(groups = { "functional" })
    public void afterClass() throws Exception {
        Camille camille = CamilleEnvironment.getCamille();
        Path path = PathBuilder.buildCustomerSpacePath(CamilleEnvironment.getPodId(), "contractId", "tenantId",
                "spaceId");
        camille.delete(path);
    }

    @Test(groups = "functional")
    public void getCredential() {

        String topology = configService.getTopology("contractId.tenantId.spaceId");
        Assert.assertEquals(topology, "sfdc");
    }

}
