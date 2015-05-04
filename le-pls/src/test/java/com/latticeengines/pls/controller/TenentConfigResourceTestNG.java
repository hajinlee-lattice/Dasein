package com.latticeengines.pls.controller;

import org.apache.commons.codec.digest.DigestUtils;
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
import com.latticeengines.domain.exposed.security.Ticket;
import com.latticeengines.pls.functionalframework.PlsFunctionalTestNGBase;

public class TenentConfigResourceTestNG extends PlsFunctionalTestNGBase {

    private Ticket ticket = null;

    @BeforeClass(groups = { "deployment" })
    public void setup() throws Exception {
        setupUsers();
        ticket = globalAuthenticationService.authenticateUser(adminUsername, DigestUtils.sha256Hex(adminPassword));
        String tenant1 = ticket.getTenants().get(0).getId();
        String tenant2 = ticket.getTenants().get(1).getId();
        setupDb(tenant1, tenant2);

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

    @Test(groups = { "deployment" })
    public void getSfdcTopology() {
        String topology = restTemplate.getForObject(getRestAPIHostPort()
                + "/pls/config/topology?tenantId=contractId.tenantId.spaceId", String.class);
        Assert.assertEquals(topology, "sfdc");
    }

}
