package com.latticeengines.pls.controller;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

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
import com.latticeengines.domain.exposed.ResponseDocument;
import com.latticeengines.domain.exposed.SimpleBooleanResponse;
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
        play = createDefaultPlay();
        playService.createOrUpdate(play, mainTestTenant.getId());
        switchToExternalUser();
    }

    @SuppressWarnings({ "unchecked" })
    @Test(groups = { "deployment" })
    public void testGetRecommendationAttributes() {
        ResponseDocument<Map<String, String>> recResponse = restTemplate.getForObject( //
                getRestAPIHostPort() + "/pls/dante/attributes/recommendationattributes", //
                ResponseDocument.class);

        Assert.assertNotNull(recResponse);
        Assert.assertNotNull(recResponse.getResult());
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

        SimpleBooleanResponse createResponse = restTemplate.postForObject( //
                getRestAPIHostPort() + "/pls/dante/talkingpoints", //
                tps, //
                SimpleBooleanResponse.class);
        Assert.assertNotNull(createResponse);
        Assert.assertNull(createResponse.getErrors());

        ResponseDocument<List> playTpsResponse = restTemplate.getForObject( //
                getRestAPIHostPort() + "/pls/dante/talkingpoints/play/" + play.getName(), //
                ResponseDocument.class);

        Assert.assertNotNull(playTpsResponse);
        Assert.assertEquals(playTpsResponse.getResult().size(), 2);
        tps = JsonUtils.convertList(playTpsResponse.getResult(), TalkingPointDTO.class);
        Assert.assertNotNull(tps);
        Assert.assertNotEquals(tps.get(0).getName(), "plsDeploymentTestTP1");
        Assert.assertNotEquals(tps.get(1).getName(), "plsDeploymentTestTP2");

        tps.get(0).setOffset(2);
        tps.get(1).setOffset(1);

        createResponse = restTemplate.postForObject( //
                getRestAPIHostPort() + "/pls/dante/talkingpoints", //
                tps, //
                SimpleBooleanResponse.class);
        Assert.assertNotNull(createResponse);
        Assert.assertNull(createResponse.getErrors());

        playTpsResponse = restTemplate.getForObject( //
                getRestAPIHostPort() + "/pls/dante/talkingpoints/play/" + play.getName(), //
                ResponseDocument.class);

        Assert.assertNotNull(playTpsResponse);
        Assert.assertEquals(playTpsResponse.getResult().size(), 2);
        tps = JsonUtils.convertList(playTpsResponse.getResult(), TalkingPointDTO.class);
        Assert.assertNotNull(tps);
        Assert.assertEquals(tps.get(0).getOffset(), 2);
        Assert.assertEquals(tps.get(1).getOffset(), 1);
    }

    @SuppressWarnings({ "unchecked" })
    @Test(groups = { "deployment" }, dependsOnMethods = { "testTalkingPointsCrud" })
    public void testPreviewAndPublish() {
        String raw = restTemplate.getForObject( //
                getRestAPIHostPort() + "/pls/dante/talkingpoints/preview?playName=" + play.getName(), //
                String.class);
        ResponseDocument<TalkingPointPreview> tpPreviewResponse = JsonUtils.deserialize(raw,
                new TypeReference<ResponseDocument<TalkingPointPreview>>() {
                });
        Assert.assertNotNull(tpPreviewResponse);
        Assert.assertNull(tpPreviewResponse.getErrors());
        TalkingPointPreview tpPreview = tpPreviewResponse.getResult();
        Assert.assertNotNull(tpPreview);
        Assert.assertEquals(tpPreview.getNotionObject().getTalkingPoints().size(), 2);

        SimpleBooleanResponse publishResponse = restTemplate.postForObject( //
                getRestAPIHostPort() + "/pls/dante/talkingpoints/publish?playName=" + play.getName(), //
                null, SimpleBooleanResponse.class);

        Assert.assertNotNull(publishResponse);
        Assert.assertNull(publishResponse.getErrors());

        restTemplate.delete(
                getRestAPIHostPort() + "/pls/dante/talkingpoints/"
                        + tpPreview.getNotionObject().getTalkingPoints().get(0).getBaseExternalID(),
                ResponseDocument.class);
        restTemplate.delete(
                getRestAPIHostPort() + "/pls/dante/talkingpoints/"
                        + tpPreview.getNotionObject().getTalkingPoints().get(1).getBaseExternalID(),
                ResponseDocument.class);

        ResponseDocument<List> playTpsResponse = restTemplate.getForObject( //
                getRestAPIHostPort() + "/pls/dante/talkingpoints/play/" + play.getName(), //
                ResponseDocument.class);

        Assert.assertNotNull(playTpsResponse);
        Assert.assertEquals(playTpsResponse.getResult().size(), 0);

        raw = restTemplate.getForObject( //
                getRestAPIHostPort() + "/pls/dante/talkingpoints/preview?playName=" + play.getName(), //
                String.class);
        tpPreviewResponse = JsonUtils.deserialize(raw,
                new TypeReference<ResponseDocument<TalkingPointPreview>>() {
                });

        Assert.assertNotNull(tpPreviewResponse);
        Assert.assertNull(tpPreviewResponse.getErrors());
        tpPreview = tpPreviewResponse.getResult();
        Assert.assertNotNull(tpPreview);
        Assert.assertEquals(tpPreview.getNotionObject().getTalkingPoints().size(), 0);

        publishResponse = restTemplate.postForObject( //
                getRestAPIHostPort() + "/pls/dante/talkingpoints/publish?playName=" + play.getName(), //
                null, SimpleBooleanResponse.class);

        Assert.assertNotNull(publishResponse);
        Assert.assertNull(publishResponse.getErrors());
    }

    @SuppressWarnings({ "unchecked" })
    @Test(groups = { "deployment" }, dependsOnMethods = { "testPreviewAndPublish" })
    public void testDantePreviewResources() {
        String raw = restTemplate.getForObject( //
                getRestAPIHostPort() + "/pls/dante/talkingpoints/previewresources", //
                String.class);
        ResponseDocument<DantePreviewResources> previewResponse = JsonUtils.deserialize(raw,
                new TypeReference<ResponseDocument<DantePreviewResources>>() {
                });
        Assert.assertNotNull(previewResponse);
        Assert.assertNotNull(previewResponse.getResult());

        SimpleBooleanResponse publishResponse = restTemplate.postForObject( //
                getRestAPIHostPort() + "/pls/dante/talkingpoints/publish?playName=" + play.getName(), //
                null, SimpleBooleanResponse.class);

        Assert.assertNotNull(publishResponse);
        Assert.assertNull(publishResponse.getErrors());
        DantePreviewResources dpr = previewResponse.getResult();
        Assert.assertNotNull(dpr);
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
