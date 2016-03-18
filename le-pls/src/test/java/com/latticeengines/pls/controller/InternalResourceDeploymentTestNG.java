package com.latticeengines.pls.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.camille.exposed.Camille;
import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.domain.exposed.ResponseDocument;
import com.latticeengines.domain.exposed.admin.CRMTopology;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.camille.Path;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.pls.functionalframework.PlsDeploymentTestNGBase;
import com.latticeengines.pls.service.TenantConfigService;

public class InternalResourceDeploymentTestNG extends PlsDeploymentTestNGBase {

    @Autowired
    private InternalResource internalResource;

    @Autowired
    private TenantConfigService tenantConfigService;

    // TODO: to enable this test, we need a REST endpoint for ZK operations, and
    // tenantConfigService
    @SuppressWarnings("rawtypes")
    @Test(groups = "deployment", enabled = false)
    public void provisionThroughTenantConsole() throws Exception {
        String tenantId = internalResource.getTestTenantIds().get(1);

        final String SPACE_CONFIGURATION_ZNODE = "/SpaceConfiguration";
        final String TOPOLOGY_ZNODE = "/Topology";

        Camille camille = CamilleEnvironment.getCamille();
        CustomerSpace customerSpace = CustomerSpace.parse(tenantId);
        Path path = PathBuilder.buildCustomerSpacePath(CamilleEnvironment.getPodId(), customerSpace.getContractId(),
                customerSpace.getTenantId(), customerSpace.getSpaceId()).append(
                new Path(SPACE_CONFIGURATION_ZNODE + TOPOLOGY_ZNODE));
        try {
            camille.delete(path);
        } catch (Exception e) {
            // ignore
        }

        try {
            tenantConfigService.getTopology(tenantId);
        } catch (LedpException e) {
            Assert.assertEquals(e.getCode(), LedpCode.LEDP_18033,
                    "Should get 18033 (can not get tenant's topology) error.");
        }

        HttpHeaders headers = new HttpHeaders();
        headers.add("Content-Type", "application/json");
        headers.add("Accept", "application/json");
        headers.add("MagicAuthentication", "Security through obscurity!");
        HttpEntity<String> requestEntity = new HttpEntity<>("", headers);
        ResponseEntity<ResponseDocument> responseEntity = magicRestTemplate.exchange(getDeployedRestAPIHostPort()
                + "/pls/internal/testtenants", HttpMethod.PUT, requestEntity, ResponseDocument.class);
        ResponseDocument response = responseEntity.getBody();
        Assert.assertTrue(response.isSuccess());

        CRMTopology topology = tenantConfigService.getTopology(tenantId);
        Assert.assertNotNull(topology);
        Assert.assertEquals(topology, CRMTopology.ELOQUA);
    }
}
