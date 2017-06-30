package com.latticeengines.pls.controller;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.web.client.RestTemplate;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.latticeengines.common.exposed.util.HttpClientUtils;
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

    @SuppressWarnings({ "unchecked" })
    @Test(groups = { "deployment" })
    public void testTalkingPoints() {
        switchToExternalUser();

        ResponseDocument<Map<String, String>> recResponse = restTemplate.getForObject( //
                getRestAPIHostPort() + "/pls/dante/attributes/recommendationattributes", //
                ResponseDocument.class);

        Assert.assertNotNull(recResponse);
        Assert.assertNotNull(recResponse.getResult());

        List<DanteTalkingPoint> dtps = new ArrayList<>();
        DanteTalkingPoint dtp = new DanteTalkingPoint();
        dtp.setCustomerID(mainTestTenant.getId());
        dtp.setExternalID("plsDeploymentTestTP1");
        dtp.setPlayExternalID("testPLSPlayExtID");
        dtp.setValue("PLS Deployment Test Talking Point no 1");
        dtps.add(dtp);

        DanteTalkingPoint dtp1 = new DanteTalkingPoint();
        dtp1.setCustomerID(mainTestTenant.getId());
        dtp1.setExternalID("plsDeploymentTestTP2");
        dtp1.setPlayExternalID("testPLSPlayExtID");
        dtp1.setValue("PLS Deployment Test Talking Point no 2");
        dtps.add(dtp1);

        ResponseDocument<?> createResponse = restTemplate.postForObject( //
                getRestAPIHostPort() + "/pls/dante/talkingpoints/", //
                dtps, //
                ResponseDocument.class);
        Assert.assertNotNull(createResponse);
        Assert.assertNull(recResponse.getErrors());

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

        ObjectMapper objMapper = new ObjectMapper();
        DantePreviewResources dpr = objMapper.convertValue(previewResponse.getResult(),
                new TypeReference<DantePreviewResources>() {
                });

        Assert.assertNotNull(dpr.getDanteUrl());
        Assert.assertNotNull(dpr.getServerUrl());
        Assert.assertNotNull(dpr.getoAuthToken());

        String url = dpr.getServerUrl() + "/tenants/oauthtotenant";
        HttpHeaders headers = new HttpHeaders();
        headers.add("Authorization", "Bearer " + dpr.getoAuthToken());
        HttpEntity<String> entity = new HttpEntity<>("parameters", headers);

        RestTemplate pmApiRestTemplate = HttpClientUtils.newRestTemplate();
        String tenantNameViaToken = pmApiRestTemplate.exchange(url, HttpMethod.GET, entity, String.class).getBody();
        Assert.assertNotNull(tenantNameViaToken);
        Assert.assertEquals(tenantNameViaToken, mainTestTenant.getId());
    }
}
