package com.latticeengines.pls.service.impl;

import org.apache.zookeeper.ZooDefs;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.baton.exposed.service.BatonService;
import com.latticeengines.baton.exposed.service.impl.BatonServiceImpl;
import com.latticeengines.camille.exposed.Camille;
import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.domain.exposed.admin.CRMTopology;
import com.latticeengines.domain.exposed.camille.Document;
import com.latticeengines.domain.exposed.camille.Path;
import com.latticeengines.domain.exposed.camille.lifecycle.CustomerSpaceInfo;
import com.latticeengines.domain.exposed.camille.lifecycle.CustomerSpaceProperties;
import com.latticeengines.pls.functionalframework.PlsFunctionalTestNGBase;
import com.latticeengines.pls.service.TenantConfigService;

public class TenantConfigServiceImplTestNG extends PlsFunctionalTestNGBase {

    private final static String contractId = "PLSTenantConfig";
    private final static String tenantId = "PLSTenantConfigTenant";
    private final static String spaceId = "spaceId";
    private static String PLSTenantId;

    @Autowired
    private TenantConfigService configService;

    private BatonService batonService = new BatonServiceImpl();

    @BeforeClass(groups = { "functional" })
    public void setup() throws Exception {
        Camille camille = CamilleEnvironment.getCamille();
        Path path = PathBuilder.buildContractPath(CamilleEnvironment.getPodId(), contractId);
        try {
            camille.delete(path);
        } catch (Exception ex) {
            // ignore
        }

        CustomerSpaceProperties properties = new CustomerSpaceProperties();
        CustomerSpaceInfo spaceInfo = new CustomerSpaceInfo(properties, "");
        batonService.createTenant(contractId, tenantId, spaceId, spaceInfo);

        path = PathBuilder.buildCustomerSpacePath(CamilleEnvironment.getPodId(), contractId, tenantId, spaceId);

        path = path.append(new Path("/SpaceConfiguration"));
        camille.create(path, ZooDefs.Ids.OPEN_ACL_UNSAFE);

        path = path.append(new Path("/Topology"));
        camille.create(path, new Document("SFDC"), ZooDefs.Ids.OPEN_ACL_UNSAFE);

        PLSTenantId = String.format("%s.%s.%s", contractId, tenantId, spaceId);
    }

    @AfterClass(groups = { "functional" })
    public void afterClass() throws Exception {
        Camille camille = CamilleEnvironment.getCamille();
        Path path = PathBuilder.buildContractPath(CamilleEnvironment.getPodId(), contractId);
        camille.delete(path);
    }

    @Test(groups = "functional")
    public void getCredential() {
        CRMTopology topology = configService.getTopology(PLSTenantId);
        Assert.assertEquals(topology, CRMTopology.SFDC);
    }

}
