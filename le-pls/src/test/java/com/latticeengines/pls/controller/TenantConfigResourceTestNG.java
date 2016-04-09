package com.latticeengines.pls.controller;

import java.io.IOException;

import org.apache.zookeeper.ZooDefs;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.latticeengines.baton.exposed.service.BatonService;
import com.latticeengines.camille.exposed.Camille;
import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.domain.exposed.admin.CRMTopology;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.camille.Document;
import com.latticeengines.domain.exposed.camille.Path;
import com.latticeengines.domain.exposed.camille.lifecycle.CustomerSpaceInfo;
import com.latticeengines.domain.exposed.camille.lifecycle.CustomerSpaceProperties;
import com.latticeengines.pls.functionalframework.PlsFunctionalTestNGBase;

public class TenantConfigResourceTestNG extends PlsFunctionalTestNGBase {

    private String PlsTenantId;

    @Autowired
    private BatonService batonService;

    @BeforeClass(groups = { "functional" })
    public void setup() throws Exception {
        super.setup();

        PlsTenantId = mainTestTenant.getId();
        String contractId = CustomerSpace.parse(PlsTenantId).getContractId();
        String tenantId = CustomerSpace.parse(PlsTenantId).getTenantId();
        String spaceId = CustomerSpace.parse(PlsTenantId).getSpaceId();

        Camille camille = CamilleEnvironment.getCamille();
        Path path = PathBuilder.buildContractPath(CamilleEnvironment.getPodId(), tenantId);
        try {
            camille.delete(path);
        } catch (Exception ex) {
            // ignore
        }

        CustomerSpaceProperties properties = new CustomerSpaceProperties();
        CustomerSpaceInfo spaceInfo = new CustomerSpaceInfo(properties, "");
        batonService.createTenant(tenantId, tenantId, spaceId, spaceInfo);

        path = PathBuilder.buildCustomerSpacePath(CamilleEnvironment.getPodId(), contractId, tenantId, spaceId);

        path = path.append(new Path("/SpaceConfiguration"));
        if (!camille.exists(path)) {
            camille.create(path, ZooDefs.Ids.OPEN_ACL_UNSAFE);
        }

        path = path.append(new Path("/Topology"));
        if (camille.exists(path)) { camille.delete(path); }
        camille.create(path, new Document(CRMTopology.SFDC.name()), ZooDefs.Ids.OPEN_ACL_UNSAFE);

    }

    @AfterClass(groups = { "functional" })
    public void afterClass() throws Exception {
        Camille camille = CamilleEnvironment.getCamille();
        Path path = PathBuilder.buildContractPath(CamilleEnvironment.getPodId(), CustomerSpace.parse(PlsTenantId).getContractId());
        camille.delete(path);
    }

    @Test(groups = { "functional" })
    public void getSfdcTopology() throws IOException {
        switchToSuperAdmin();
        String response = restTemplate.getForObject(getRestAPIHostPort()
                + "/pls/config/topology?tenantId=" + PlsTenantId, String.class);
        ObjectMapper mapper = new ObjectMapper();
        JsonNode json = mapper.readTree(response);
        Assert.assertEquals(CRMTopology.fromName(json.get("Topology").asText()), CRMTopology.SFDC);
    }

}
