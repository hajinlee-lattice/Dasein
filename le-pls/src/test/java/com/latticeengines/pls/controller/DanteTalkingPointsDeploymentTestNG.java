package com.latticeengines.pls.controller;

import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.ResponseDocument;
import com.latticeengines.domain.exposed.dante.DantePreviewResources;
import com.latticeengines.domain.exposed.dante.DanteTalkingPoint;
import com.latticeengines.pls.functionalframework.PlsDeploymentTestNGBase;

public class DanteTalkingPointsDeploymentTestNG extends PlsDeploymentTestNGBase {
    private static final Log log = LogFactory.getLog(DanteTalkingPointsDeploymentTestNG.class);
    private String tenantId;

    @BeforeClass(groups = { "deployment" })
    public void setup() throws Exception {
        setupTestEnvironmentWithOneTenant();
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Test(groups = { "deployment" })
    public void testTalkingPoints() {
        switchToExternalUser();

        ResponseDocument<Map<String, String>> recResponse = restTemplate.getForObject( //
                getRestAPIHostPort() + "/pls/dante/attributes/recommendationattributes", //
                ResponseDocument.class);

        Assert.assertNotNull(recResponse);
        Assert.assertNotNull(recResponse.getResult());

        DanteTalkingPoint dtp = new DanteTalkingPoint();
        dtp.setCustomerID(mainTestTenant.getId());
        dtp.setExternalID("plsDeploymentTestTP1");
        dtp.setPlayExternalID("testPLSPlayExtID");
        dtp.setValue("PLS Deployment Test Talking Point no 1");

        ResponseDocument<?> createResponse = restTemplate.postForObject( //
                getRestAPIHostPort() + "/pls/dante/talkingpoints/", //
                dtp, //
                ResponseDocument.class);
        Assert.assertNotNull(createResponse);
        Assert.assertNull(recResponse.getErrors());

        dtp.setCustomerID(mainTestTenant.getId());
        dtp.setExternalID("plsDeploymentTestTP2");
        dtp.setPlayExternalID("testPLSPlayExtID");
        dtp.setValue("PLS Deployment Test Talking Point no 2");

        createResponse = restTemplate.postForObject( //
                getRestAPIHostPort() + "/pls/dante/talkingpoints/", //
                dtp, //
                ResponseDocument.class);
        Assert.assertNotNull(createResponse);
        Assert.assertNull(createResponse.getErrors());

        ResponseDocument<List<DanteTalkingPoint>> playTpsResponse = restTemplate.getForObject( //
                getRestAPIHostPort() + "/pls/dante/talkingpoints/play/testPLSPlayExtID", //
                ResponseDocument.class);

        Assert.assertNotNull(playTpsResponse);
        Assert.assertEquals(playTpsResponse.getResult().size(), 2);

        restTemplate.delete(getRestAPIHostPort() + "/pls/dante/talkingpoints/plsDeploymentTestTP1", //
                ResponseDocument.class);
        restTemplate.delete(getRestAPIHostPort() + "/pls/dante/talkingpoints/plsDeploymentTestTP2", //
                ResponseDocument.class);

        playTpsResponse = restTemplate.getForObject( //
                getRestAPIHostPort() + "/pls/dante/talkingpoints/play/testPLSPlayExtID", //
                ResponseDocument.class);

        Assert.assertNotNull(playTpsResponse);
        Assert.assertEquals(playTpsResponse.getResult().size(), 0);

        ResponseDocument<DantePreviewResources> previewResponse = restTemplate.getForObject( //
                getRestAPIHostPort() + "/pls/dante/talkingpoints/previewresources", //
                ResponseDocument.class);

        Assert.assertNotNull(previewResponse);
        Assert.assertNotNull(previewResponse.getResult());
    }
}
