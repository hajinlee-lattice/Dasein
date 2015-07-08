package com.latticeengines.pls.controller;

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

public class TenentConfigResourceTestNG extends PlsFunctionalTestNGBase {

    @BeforeClass(groups = { "deployment" })
    public void setup() throws Exception {
        setUpMarketoEloquaTestEnvironment();

        Camille camille = CamilleEnvironment.getCamille();
        Path path = PathBuilder.buildCustomerSpacePath(CamilleEnvironment.getPodId(), "contractId", "tenantId",
                "spaceId");
        try {
            camille.delete(path);
        } catch (Exception ex) {
            // ignore
        }
        CustomerSpaceProperties properties = new CustomerSpaceProperties();
        CustomerSpaceInfo spaceInfo = new CustomerSpaceInfo(properties, "");

        SpaceLifecycleManager.create("contractId", "tenantId", "spaceId", spaceInfo);
    }

    @AfterClass(groups = { "deployment" })
    public void afterClass() throws Exception {
        Camille camille = CamilleEnvironment.getCamille();
        Path path = PathBuilder.buildCustomerSpacePath(CamilleEnvironment.getPodId(), "contractId", "tenantId",
                "spaceId");
        camille.delete(path);
    }

    @Test(groups = { "deployment" })
    public void getSfdcTopology() {
        String topology = restTemplate.getForObject(getRestAPIHostPort()
                + "/pls/config/topology?tenantId=contractId.tenantId.spaceId", String.class);
        Assert.assertEquals(topology, "SFDC");
    }

}
