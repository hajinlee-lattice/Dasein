package com.latticeengines.pls.controller;

import java.util.ArrayList;
import java.util.List;

import javax.inject.Inject;

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
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.dante.DantePreviewResources;
import com.latticeengines.domain.exposed.dante.TalkingPointAttribute;
import com.latticeengines.domain.exposed.dante.TalkingPointPreview;
import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.domain.exposed.multitenant.TalkingPointDTO;
import com.latticeengines.domain.exposed.pls.Play;
import com.latticeengines.domain.exposed.pls.RatingEngine;
import com.latticeengines.domain.exposed.pls.RatingEngineType;
import com.latticeengines.domain.exposed.query.frontend.FrontEndRestriction;
import com.latticeengines.metadata.service.SegmentService;
import com.latticeengines.pls.functionalframework.PlsDeploymentTestNGBase;
import com.latticeengines.proxy.exposed.cdl.PlayProxy;
import com.latticeengines.proxy.exposed.cdl.RatingEngineProxy;
import com.latticeengines.db.exposed.util.MultiTenantContext;

public class TalkingPointsDeploymentTestNG extends PlsDeploymentTestNGBase {

    @SuppressWarnings("unused")
    private static final Logger log = LoggerFactory.getLogger(TalkingPointsDeploymentTestNG.class);

    private static final String PLAY_DISPLAY_NAME = "Test TP Plays hard";
    private static final String SEGMENT_NAME = "testTPSegment";
    private static final String CREATED_BY = "lattice@lattice-engines.com";
    private static Play play;

    @Autowired
    private SegmentService segmentService;

    private RatingEngine ratingEngine1;
    private MetadataSegment segment;

    @Autowired
    private PlayProxy playProxy;

    @Inject
    private RatingEngineProxy ratingEngineProxy;

    @BeforeClass(groups = { "deployment" })
    public void setup() throws Exception {
        setupTestEnvironmentWithOneTenant();
        MultiTenantContext.setTenant(mainTestTenant);
        segment = new MetadataSegment();
        segment.setAccountFrontEndRestriction(new FrontEndRestriction());
        segment.setDisplayName(SEGMENT_NAME);
        MetadataSegment createdSegment = segmentService
                .createOrUpdateSegment(CustomerSpace.parse(mainTestTenant.getId()).toString(), segment);
        MetadataSegment retrievedSegment = segmentService
                .findByName(CustomerSpace.parse(mainTestTenant.getId()).toString(), createdSegment.getName());
        Assert.assertNotNull(retrievedSegment);

        ratingEngine1 = new RatingEngine();
        ratingEngine1.setSegment(retrievedSegment);
        ratingEngine1.setCreatedBy(CREATED_BY);
        ratingEngine1.setType(RatingEngineType.RULE_BASED);
        RatingEngine createdRatingEngine = ratingEngineProxy.createOrUpdateRatingEngine(mainTestTenant.getId(),
                ratingEngine1);
        Assert.assertNotNull(createdRatingEngine);
        ratingEngine1.setId(createdRatingEngine.getId());

        Play newPlay = createDefaultPlay();
        play = playProxy.createOrUpdatePlay(mainTestTenant.getId(), newPlay);
        switchToExternalUser();
    }

    @SuppressWarnings({ "unchecked" })
    @Test(groups = { "deployment" })
    public void testGetRecommendationAttributes() {
        List<TalkingPointAttribute> rawResponse = restTemplate.getForObject( //
                getRestAPIHostPort() + "/pls/dante/attributes/recommendationattributes", //
                List.class);
        List<TalkingPointAttribute> recResponse = JsonUtils.convertList(rawResponse, TalkingPointAttribute.class);
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

        play = playProxy.getPlay(mainTestTenant.getId(), play.getName());
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
        play.setDisplayName(PLAY_DISPLAY_NAME);
        play.setCreatedBy(CREATED_BY);
        RatingEngine ratingEngine = new RatingEngine();
        ratingEngine.setId(ratingEngine1.getId());
        play.setRatingEngine(ratingEngine);
        return play;
    }

    @AfterTest(groups = { "deployment" })
    public void teardown() throws Exception {
        MultiTenantContext.setTenant(mainTestTenant);
        playProxy.deletePlay(mainTestTenant.getId(), play.getName());
    }
}
