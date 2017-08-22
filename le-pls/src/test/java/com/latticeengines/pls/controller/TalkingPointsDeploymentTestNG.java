package com.latticeengines.pls.controller;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.web.client.RestTemplate;
import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.fasterxml.jackson.core.type.TypeReference;
import com.latticeengines.common.exposed.util.HttpClientUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.dante.DanteAttribute;
import com.latticeengines.domain.exposed.dante.DantePreviewResources;
import com.latticeengines.domain.exposed.dante.TalkingPointPreview;
import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.domain.exposed.pls.Play;
import com.latticeengines.domain.exposed.pls.TalkingPointDTO;
import com.latticeengines.pls.functionalframework.PlsDeploymentTestNGBase;
import com.latticeengines.pls.service.PlayService;
import com.latticeengines.security.exposed.util.MultiTenantContext;

public class TalkingPointsDeploymentTestNG extends PlsDeploymentTestNGBase {

    @SuppressWarnings("unused")
    private static final Logger log = LoggerFactory.getLogger(TalkingPointsDeploymentTestNG.class);

    private static final String PLAY_DISPLAY_NAME = "Test TP Plays hard";
    private static final String SEGMENT_NAME = "testTPSegment";
    private static final String CREATED_BY = "lattice@lattice-engines.com";
    private static Play play;

    @Autowired
    private PlayService playService;

    @BeforeClass(groups = { "deployment" })
    public void setup() throws Exception {
        setupTestEnvironmentWithOneTenant();
        MultiTenantContext.setTenant(mainTestTenant);
        play = createDefaultPlay();
        playService.createOrUpdate(play, mainTestTenant.getId());
        switchToExternalUser();
    }

    @SuppressWarnings({ "unchecked" })
    @Test(groups = { "deployment" })
    public void testGetRecommendationAttributes() {
        List<DanteAttribute> rawResponse = restTemplate.getForObject( //
                getRestAPIHostPort() + "/pls/dante/attributes/recommendationattributes", //
                List.class);
        List<DanteAttribute> recResponse = JsonUtils.convertList(rawResponse, DanteAttribute.class);
        Assert.assertNotNull(recResponse);
    }

    @SuppressWarnings({ "unchecked" })
    @Test(groups = { "deployment" })
    public void testTalkingPointsCrud() {
        List<TalkingPointDTO> tps = new ArrayList<>();
        TalkingPointDTO tp = new TalkingPointDTO();
        tp.setName("plsDeploymentTestTP1");
        tp.setPlayName(play.getName());
        tp.setOffset(1);
        tp.setTitle("Test TP Title");
        tp.setContent("PLS Deployment Test Talking Point no 1");
        tps.add(tp);

        TalkingPointDTO tp1 = new TalkingPointDTO();

        tp1.setName("plsDeploymentTestTP2");
        tp1.setPlayName(play.getName());
        tp1.setOffset(2);
        tp1.setTitle("Test TP2 Title");
        tp1.setContent("PLS Deployment Test Talking Point no 2");
        tps.add(tp1);

        List<TalkingPointDTO> createResponseRaw = restTemplate.postForObject( //
                getRestAPIHostPort() + "/pls/dante/talkingpoints", //
                tps, //
                List.class);
        List<TalkingPointDTO> createResponse = JsonUtils.convertList(createResponseRaw, TalkingPointDTO.class);

        Assert.assertNotNull(createResponse);
        Assert.assertEquals(createResponse.size(), 2);

        List<TalkingPointDTO> playTpsResponse = restTemplate.getForObject( //
                getRestAPIHostPort() + "/pls/dante/talkingpoints/play/" + play.getName(), //
                List.class);

        Assert.assertNotNull(playTpsResponse);
        tps = JsonUtils.convertList(playTpsResponse, TalkingPointDTO.class);
        Assert.assertNotNull(tps);
        Assert.assertEquals(tps.size(), 2);
        Assert.assertEquals(tps.get(0).getName(), createResponse.get(0).getName());
        Assert.assertEquals(tps.get(1).getName(), createResponse.get(1).getName());

        tps.get(0).setOffset(2);
        tps.get(1).setOffset(1);

        createResponse = restTemplate.postForObject( //
                getRestAPIHostPort() + "/pls/dante/talkingpoints", //
                tps, //
                List.class);
        Assert.assertNotNull(createResponse);

        playTpsResponse = restTemplate.getForObject( //
                getRestAPIHostPort() + "/pls/dante/talkingpoints/play/" + play.getName(), //
                List.class);

        Assert.assertNotNull(playTpsResponse);
        tps = JsonUtils.convertList(playTpsResponse, TalkingPointDTO.class);
        Assert.assertNotNull(tps);
        Assert.assertEquals(tps.size(), 2);
        Assert.assertEquals(tps.get(0).getOffset(), 2);
        Assert.assertEquals(tps.get(1).getOffset(), 1);
    }

    @SuppressWarnings({ "unchecked" })
    @Test(groups = { "deployment" }, dependsOnMethods = { "testTalkingPointsCrud" })
    public void testPreviewAndPublish() {
        String raw = restTemplate.getForObject( //
                getRestAPIHostPort() + "/pls/dante/talkingpoints/preview?playName=" + play.getName(), //
                String.class);
        TalkingPointPreview tpPreview = JsonUtils.deserialize(raw, new TypeReference<TalkingPointPreview>() {
        });

        Assert.assertNotNull(tpPreview);
        Assert.assertEquals(tpPreview.getNotionObject().getTalkingPoints().size(), 2);
        Assert.assertEquals(tpPreview.getNotionObject().getTalkingPoints().get(0).getOffset(), 1);
        Assert.assertEquals(tpPreview.getNotionObject().getTalkingPoints().get(1).getOffset(), 2);

        restTemplate.postForObject( //
                getRestAPIHostPort() + "/pls/dante/talkingpoints/publish?playName=" + play.getName(), //
                null, String.class);

        play = playService.getPlayByName(play.getName());
        Assert.assertNotNull(play.getLastTalkingPointPublishTime());

        restTemplate.delete(getRestAPIHostPort() + "/pls/dante/talkingpoints/"
                + tpPreview.getNotionObject().getTalkingPoints().get(0).getBaseExternalID());
        restTemplate.delete(getRestAPIHostPort() + "/pls/dante/talkingpoints/"
                + tpPreview.getNotionObject().getTalkingPoints().get(1).getBaseExternalID());

        List<TalkingPointDTO> playTpsResponse = restTemplate.getForObject( //
                getRestAPIHostPort() + "/pls/dante/talkingpoints/play/" + play.getName(), //
                List.class);

        Assert.assertNotNull(playTpsResponse);
        Assert.assertEquals(playTpsResponse.size(), 0);

        List<TalkingPointDTO> rawList = restTemplate.postForObject( //
                getRestAPIHostPort() + "/pls/dante/talkingpoints/revert?playName=" + play.getName() //
                        + "&customerSpace=" + mainTestTenant.getId(), //
                null, List.class);

        playTpsResponse = JsonUtils.convertList(rawList, TalkingPointDTO.class);

        Assert.assertNotNull(playTpsResponse);
        Assert.assertEquals(playTpsResponse.size(), 2);
        Assert.assertEquals(playTpsResponse.get(0).getName(),
                tpPreview.getNotionObject().getTalkingPoints().get(1).getBaseExternalID());
        Assert.assertEquals(playTpsResponse.get(1).getName(),
                tpPreview.getNotionObject().getTalkingPoints().get(0).getBaseExternalID());

        restTemplate.delete(getRestAPIHostPort() + "/pls/dante/talkingpoints/"
                + tpPreview.getNotionObject().getTalkingPoints().get(0).getBaseExternalID());
        restTemplate.delete(getRestAPIHostPort() + "/pls/dante/talkingpoints/"
                + tpPreview.getNotionObject().getTalkingPoints().get(1).getBaseExternalID());

        playTpsResponse = restTemplate.getForObject( //
                getRestAPIHostPort() + "/pls/dante/talkingpoints/play/" + play.getName(), //
                List.class);

        Assert.assertNotNull(playTpsResponse);
        Assert.assertEquals(playTpsResponse.size(), 0);

        raw = restTemplate.getForObject( //
                getRestAPIHostPort() + "/pls/dante/talkingpoints/preview?playName=" + play.getName(), //
                String.class);
        tpPreview = JsonUtils.deserialize(raw, new TypeReference<TalkingPointPreview>() {
        });

        Assert.assertNotNull(tpPreview);
        Assert.assertEquals(tpPreview.getNotionObject().getTalkingPoints().size(), 0);

        restTemplate.postForObject( //
                getRestAPIHostPort() + "/pls/dante/talkingpoints/publish?playName=" + play.getName(), //
                null, String.class);
    }

    @SuppressWarnings({ "unchecked" })
    @Test(groups = { "deployment" }, dependsOnMethods = { "testPreviewAndPublish" })
    public void testDantePreviewResources() {
        String raw = restTemplate.getForObject( //
                getRestAPIHostPort() + "/pls/dante/talkingpoints/previewresources", //
                String.class);
        DantePreviewResources previewResources = JsonUtils.deserialize(raw, new TypeReference<DantePreviewResources>() {
        });
        Assert.assertNotNull(previewResources);
        Assert.assertNotNull(previewResources.getDanteUrl());
        Assert.assertNotNull(previewResources.getServerUrl());
        Assert.assertNotNull(previewResources.getoAuthToken());

        restTemplate.postForObject( //
                getRestAPIHostPort() + "/pls/dante/talkingpoints/publish?playName=" + play.getName(), //
                null, String.class);

        String url = previewResources.getServerUrl() + "/playmaker/tenants/oauthtotenant";
        HttpHeaders headers = new HttpHeaders();
        headers.add("Authorization", "Bearer " + previewResources.getoAuthToken());
        HttpEntity<String> entity = new HttpEntity<>("parameters", headers);

        RestTemplate pmApiRestTemplate = HttpClientUtils.newRestTemplate();
        String tenantNameViaToken = pmApiRestTemplate.exchange(url, HttpMethod.GET, entity, String.class).getBody();
        Assert.assertNotNull(tenantNameViaToken);
        Assert.assertEquals(tenantNameViaToken, mainTestTenant.getId());
    }

    private Play createDefaultPlay() {
        Play play = new Play();
        MetadataSegment segment = new MetadataSegment();
        segment.setDisplayName(SEGMENT_NAME);
        play.setDisplayName(PLAY_DISPLAY_NAME);
        play.setSegment(segment);
        play.setSegmentName(SEGMENT_NAME);
        play.setCreatedBy(CREATED_BY);
        return play;
    }

    @AfterTest(groups = { "deployment" })
    public void teardown() throws Exception {
        MultiTenantContext.setTenant(mainTestTenant);
        playService.deleteByName(play.getName());
    }
}
