package com.latticeengines.pls.controller;

import com.latticeengines.pls.functionalframework.PlsDeploymentTestNGBase;

public class TalkingPointsDeploymentTestNG extends PlsDeploymentTestNGBase {
    // private static final Logger log =
    // LoggerFactory.getLogger(TalkingPointsDeploymentTestNG.class);
    // private static final String PLAY_DISPLAY_NAME = "Test TP Play hard";
    // private static final String SEGMENT_NAME = "testTPSegment";
    // private static final String CREATED_BY = "lattice@lattice-engines.com";
    // private static Play play;
    //
    // @Autowired
    // PlayService playService;
    //
    // @BeforeClass(groups = { "deployment" })
    // public void setup() throws Exception {
    // setupTestEnvironmentWithOneTenant();
    // play = createDefaultPlay();
    // playService.createOrUpdate(play, mainTestTenant.getId());
    // }
    //
    // @SuppressWarnings({ "unchecked" })
    // @Test(groups = { "deployment" })
    // public void testTalkingPoints() {
    // switchToExternalUser();
    //
    // ResponseDocument<Map<String, String>> recResponse =
    // restTemplate.getForObject( //
    // getRestAPIHostPort() + "/pls/dante/attributes/recommendationattributes",
    // //
    // ResponseDocument.class);
    //
    // Assert.assertNotNull(recResponse);
    // Assert.assertNotNull(recResponse.getResult());
    //
    // List<TalkingPointDTO> tps = new ArrayList<>();
    // TalkingPointDTO tp = new TalkingPointDTO();
    // tp.setName("plsDeploymentTestTP1");
    // tp.setPlayName(play.getName());
    // tp.setOffset(1);
    // tp.setTitle("Test TP Title");
    // tp.setContent("PLS Deployment Test Talking Point no 1");
    // tps.add(tp);
    //
    // TalkingPointDTO tp1 = new TalkingPointDTO();
    //
    // tp1.setName("plsDeploymentTestTP2");
    // tp1.setPlayName(play.getName());
    // tp1.setOffset(2);
    // tp1.setTitle("Test TP2 Title");
    // tp1.setContent("PLS Deployment Test Talking Point no 2");
    // tps.add(tp1);
    //
    // ResponseDocument<?> createResponse = restTemplate.postForObject( //
    // getRestAPIHostPort() + "/pls/dante/talkingpoints", //
    // tps, //
    // ResponseDocument.class);
    // Assert.assertNotNull(createResponse);
    // Assert.assertNull(recResponse.getErrors());
    //
    // ResponseDocument<List<TalkingPointDTO>> playTpsResponse =
    // restTemplate.getForObject( //
    // getRestAPIHostPort() + "/pls/dante/talkingpoints/play/" + play.getName(),
    // //
    // ResponseDocument.class);
    //
    // Assert.assertNotNull(playTpsResponse);
    // Assert.assertEquals(playTpsResponse.getResult().size(), 2);
    //
    // restTemplate.delete(getRestAPIHostPort() +
    // "/pls/dante/talkingpoints/plsDeploymentTestTP1", //
    // ResponseDocument.class);
    // restTemplate.delete(getRestAPIHostPort() +
    // "/pls/dante/talkingpoints/plsDeploymentTestTP2", //
    // ResponseDocument.class);
    //
    // playTpsResponse = restTemplate.getForObject( //
    // getRestAPIHostPort() + "/pls/dante/talkingpoints/play/" + play.getName(),
    // //
    // ResponseDocument.class);
    //
    // Assert.assertNotNull(playTpsResponse);
    // Assert.assertEquals(playTpsResponse.getResult().size(), 0);
    //
    // ResponseDocument<DantePreviewResources> previewResponse =
    // restTemplate.getForObject( //
    // getRestAPIHostPort() + "/pls/dante/talkingpoints/previewresources", //
    // ResponseDocument.class);
    //
    // Assert.assertNotNull(previewResponse);
    // Assert.assertNotNull(previewResponse.getResult());
    //
    // ObjectMapper objMapper = new ObjectMapper();
    // DantePreviewResources dpr =
    // objMapper.convertValue(previewResponse.getResult(),
    // new TypeReference<DantePreviewResources>() {
    // });
    //
    // Assert.assertNotNull(dpr.getDanteUrl());
    // Assert.assertNotNull(dpr.getServerUrl());
    // Assert.assertNotNull(dpr.getoAuthToken());
    //
    // String url = dpr.getServerUrl() + "/tenants/oauthtotenant";
    // HttpHeaders headers = new HttpHeaders();
    // headers.add("Authorization", "Bearer " + dpr.getoAuthToken());
    // HttpEntity<String> entity = new HttpEntity<>("parameters", headers);
    //
    // RestTemplate pmApiRestTemplate = HttpClientUtils.newRestTemplate();
    // String tenantNameViaToken = pmApiRestTemplate.exchange(url,
    // HttpMethod.GET, entity, String.class).getBody();
    // Assert.assertNotNull(tenantNameViaToken);
    // Assert.assertEquals(tenantNameViaToken, mainTestTenant.getId());
    // }
    //
    // private Play createDefaultPlay() {
    // Play play = new Play();
    // MetadataSegment segment = new MetadataSegment();
    // segment.setDisplayName(SEGMENT_NAME);
    // play.setDisplayName(PLAY_DISPLAY_NAME);
    // play.setSegment(segment);
    // play.setSegmentName(SEGMENT_NAME);
    // play.setCreatedBy(CREATED_BY);
    // return play;
    // }
    //
    // @AfterTest(groups = { "deployment" })
    // public void teardown() throws Exception {
    // setupTestEnvironmentWithOneTenant();
    // play = createDefaultPlay();
    // playService.createOrUpdate(play, mainTestTenant.getId());
    // }
}
