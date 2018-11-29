//package com.latticeengines.dante.controller;
//
//import java.util.ArrayList;
//import java.util.List;
//
//import org.springframework.beans.factory.annotation.Autowired;
//import org.springframework.beans.factory.annotation.Value;
//import org.springframework.http.HttpEntity;
//import org.springframework.http.HttpHeaders;
//import org.springframework.http.HttpMethod;
//import org.springframework.web.client.RestTemplate;
//import org.testng.Assert;
//import org.testng.annotations.BeforeClass;
//import org.testng.annotations.Test;
//
//import com.latticeengines.common.exposed.util.HttpClientUtils;
//import com.latticeengines.common.exposed.util.JsonUtils;
//import com.latticeengines.dante.entitymgr.PublishedTalkingPointEntityMgr;
//import com.latticeengines.dante.testframework.DanteDeploymentTestNGBase;
//import com.latticeengines.domain.exposed.cdl.DantePreviewResources;
//import com.latticeengines.domain.exposed.cdl.TalkingPointPreview;
//import com.latticeengines.domain.exposed.cdl.PublishedTalkingPoint;
//import com.latticeengines.domain.exposed.cdl.TalkingPointDTO;
//import com.latticeengines.proxy.exposed.cdl.TalkingPointProxy;
//
//public class TalkingPointResourceDeploymentTestNG extends DanteDeploymentTestNGBase {
//
//    @Autowired
//    private TalkingPointProxy talkingPointProxy;
//
//    @Autowired
//    private PublishedTalkingPointEntityMgr publishedTalkingPointEntityMgr;
//
//    @Value("${common.test.pls.url}")
//    private String internalResourceHostPort;
//
//    @BeforeClass(groups = "deployment")
//    public void setup() throws Exception {
//        super.setupRunEnvironment();
//        super.createDependences();
//    }
//
//    @Test(groups = "deployment")
//    public void testCreateUpdate() {
//        List<TalkingPointDTO> tps = new ArrayList<>();
//        TalkingPointDTO tp = new TalkingPointDTO();
//        tp.setName("plsDeploymentTestTP1");
//        tp.setPlayName(testPlay.getName());
//        tp.setOffset(1);
//        tp.setTitle("Test TP Title");
//        tp.setContent("PLS Deployment Test Talking Point no 1");
//        tps.add(tp);
//
//        TalkingPointDTO tp1 = new TalkingPointDTO();
//        tp1.setName("plsDeploymentTestTP2");
//        tp1.setPlayName(testPlay.getName());
//        tp1.setOffset(2);
//        tp1.setTitle("Test TP2 Title");
//        tp1.setContent("PLS Deployment Test Talking Point no 2");
//        tps.add(tp1);
//
//        List<TalkingPointDTO> rawcreate = talkingPointProxy.createOrUpdate(tps, mainTestCustomerSpace.toString());
//        List<TalkingPointDTO> createResponse = JsonUtils.convertList(rawcreate, TalkingPointDTO.class);
//        Assert.assertNotNull(createResponse);
//        Assert.assertEquals(createResponse.size(), 2);
//
//        List<TalkingPointDTO> raw = talkingPointProxy.findAllByPlayName(testPlay.getName());
//        List<TalkingPointDTO> playTpsResponse = JsonUtils.convertList(raw, TalkingPointDTO.class);
//
//        Assert.assertNotNull(playTpsResponse);
//        Assert.assertEquals(playTpsResponse.size(), 2);
//        Assert.assertEquals(playTpsResponse.get(0).getName(), createResponse.get(0).getName());
//        Assert.assertEquals(playTpsResponse.get(1).getName(), createResponse.get(1).getName());
//
//        playTpsResponse.get(0).setOffset(2);
//        playTpsResponse.get(1).setOffset(1);
//
//        rawcreate = talkingPointProxy.createOrUpdate(playTpsResponse, mainTestCustomerSpace.toString());
//        createResponse = JsonUtils.convertList(rawcreate, TalkingPointDTO.class);
//        Assert.assertNotNull(createResponse);
//        Assert.assertNotEquals(tps.get(0).getName(), createResponse.get(0).getName());
//        Assert.assertNotEquals(tps.get(1).getName(), createResponse.get(1).getName());
//
//        raw = talkingPointProxy.findAllByPlayName(testPlay.getName());
//        playTpsResponse = JsonUtils.convertList(raw, TalkingPointDTO.class);
//        Assert.assertNotNull(playTpsResponse);
//        Assert.assertEquals(playTpsResponse.size(), 2);
//        Assert.assertEquals(playTpsResponse.get(0).getOffset(), 2);
//        Assert.assertEquals(playTpsResponse.get(1).getOffset(), 1);
//
//        TalkingPointPreview rawPreview = talkingPointProxy.getTalkingPointPreview(testPlay.getName(),
//                mainTestCustomerSpace.toString());
//        Assert.assertEquals(rawPreview.getNotionObject().getTalkingPoints().get(0).getOffset(), 1);
//        Assert.assertEquals(rawPreview.getNotionObject().getTalkingPoints().get(1).getOffset(), 2);
//
//    }
//
//    @Test(groups = { "deployment" }, dependsOnMethods = { "testCreateUpdate" })
//    public void testPreviewAndPublish() {
//        List<TalkingPointDTO> raw = talkingPointProxy.findAllByPlayName(testPlay.getName());
//        List<TalkingPointDTO> tps = JsonUtils.convertList(raw, TalkingPointDTO.class);
//        Assert.assertEquals(tps.size(), 2);
//
//        // preview
//        TalkingPointPreview tpPreview = talkingPointProxy.getTalkingPointPreview(testPlay.getName(),
//                mainTestCustomerSpace.toString());
//        Assert.assertNotNull(tpPreview);
//        Assert.assertEquals(tpPreview.getNotionObject().getTalkingPoints().size(), 2);
//        Assert.assertEquals(tpPreview.getNotionObject().getTalkingPoints().get(0).getBaseExternalID(),
//                tps.get(1).getName());
//
//        // publish
//        talkingPointProxy.publish(testPlay.getName(), mainTestCustomerSpace.toString());
//
//        List<PublishedTalkingPoint> dtps = publishedTalkingPointEntityMgr.findAllByPlayName(testPlay.getName());
//        Assert.assertEquals(dtps.size(), 2);
//
//        Assert.assertEquals(dtps.get(0).getName(), tps.get(0).getName());
//        Assert.assertEquals(dtps.get(1).getName(), tps.get(1).getName());
//
//        // delete
//        talkingPointProxy.delete(tps.get(0).getName());
//        talkingPointProxy.delete(tps.get(1).getName());
//
//        raw = talkingPointProxy.findAllByPlayName(testPlay.getName());
//        tps = JsonUtils.convertList(raw, TalkingPointDTO.class);
//        Assert.assertEquals(tps.size(), 0);
//
//        // revert
//        raw = talkingPointProxy.revert(testPlay.getName(), mainTestCustomerSpace.toString());
//        tps = JsonUtils.convertList(raw, TalkingPointDTO.class);
//        Assert.assertEquals(tps.size(), 2);
//        Assert.assertEquals(dtps.get(0).getName(), tps.get(0).getName());
//        Assert.assertEquals(dtps.get(1).getName(), tps.get(1).getName());
//
//        // delete
//        talkingPointProxy.delete(tps.get(0).getName());
//        talkingPointProxy.delete(tps.get(1).getName());
//
//        raw = talkingPointProxy.findAllByPlayName(testPlay.getName());
//        tps = JsonUtils.convertList(raw, TalkingPointDTO.class);
//        Assert.assertEquals(tps.size(), 0);
//
//        // preview
//        tpPreview = talkingPointProxy.getTalkingPointPreview(testPlay.getName(), mainTestCustomerSpace.toString());
//        Assert.assertNotNull(tpPreview);
//        Assert.assertEquals(tpPreview.getNotionObject().getTalkingPoints().size(), 0);
//
//        // publish
//        talkingPointProxy.publish(testPlay.getName(), mainTestCustomerSpace.toString());
//
//        dtps = publishedTalkingPointEntityMgr.findAllByPlayName(testPlay.getName());
//        Assert.assertEquals(dtps.size(), 0);
//    }
//
//    @Test(groups = "deployment")
//    public void testDanteOauth() {
//        DantePreviewResources previewResources = talkingPointProxy.getPreviewResources(mainTestTenant.getId());
//
//        Assert.assertNotNull(previewResources);
//        Assert.assertNotNull(previewResources.getDanteUrl());
//        Assert.assertNotNull(previewResources.getoAuthToken());
//        Assert.assertNotNull(previewResources.getServerUrl());
//
//        String url = previewResources.getServerUrl() + "/ulysses/generic/oauthtotenant";
//        RestTemplate restTemplate = HttpClientUtils.newRestTemplate();
//        HttpHeaders headers = new HttpHeaders();
//        headers.add("Authorization", "Bearer " + previewResources.getoAuthToken());
//        HttpEntity<String> entity = new HttpEntity<>("parameters", headers);
//
//        String tenantNameViaToken = restTemplate.exchange(url, HttpMethod.GET, entity, String.class).getBody();
//        Assert.assertNotNull(tenantNameViaToken);
//        Assert.assertEquals(tenantNameViaToken, mainTestTenant.getId());
//
//        DantePreviewResources previewResources1 = talkingPointProxy.getPreviewResources(mainTestTenant.getId());
//        Assert.assertEquals(previewResources.getoAuthToken(), previewResources1.getoAuthToken());
//    }
//
//}
