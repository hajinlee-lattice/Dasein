package com.latticeengines.dante.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.support.DirtiesContextTestExecutionListener;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.BeforeClass;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.latticeengines.dante.entitymgr.DanteTalkingPointEntityMgr;
import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.domain.exposed.pls.Play;
import com.latticeengines.proxy.exposed.dante.TalkingPointProxy;
import com.latticeengines.proxy.exposed.pls.InternalResourceRestApiProxy;

@TestExecutionListeners({ DirtiesContextTestExecutionListener.class })
@ContextConfiguration(locations = { "classpath:test-dante-context.xml" })
public class TalkingPointResourceDeploymentTestNG extends AbstractTestNGSpringContextTests {

    private Play createDefaultPlay() {
        Play play = new Play();
        MetadataSegment segment = new MetadataSegment();
        segment.setName(SEGMENT_NAME);
        segment.setDisplayName(SEGMENT_NAME);
        play.setName(PLAY_NAME);
        play.setDisplayName(PLAY_DISPLAY_NAME);
        play.setSegment(segment);
        play.setSegmentName(SEGMENT_NAME);
        play.setCreatedBy(CREATED_BY);
        return play;
    }

    private static final String PLAY_NAME = "tpTestPlay";
    private static final String PLAY_DISPLAY_NAME = "Play hard";
    private static final String SEGMENT_NAME = "segment";
    private static final String CREATED_BY = "lattice@lattice-engines.com";

    private final ObjectMapper objMapper = new ObjectMapper();

    @Autowired
    private TalkingPointProxy talkingPointProxy;

    @Autowired
    private DanteTalkingPointEntityMgr danteTalkingPointEntityMgr;

    @Autowired
    private InternalResourceRestApiProxy plsProxy;

    private final String externalID = "talkingPointDepTestExtID";

    @BeforeClass(groups = "deployment")
    public void setup() {

        Play play = plsProxy.createOrUpdatePlay(null, createDefaultPlay());
        // DanteTalkingPoint dtp =
        // danteTalkingPointEntityMgr.findByExternalID(externalID);
        // if (dtp != null)
        // danteTalkingPointEntityMgr.delete(dtp);
    }
    //
    // @Test(groups = "deployment")
    // public void testCreateFromService() {
    // List<DanteTalkingPoint> dtps = new ArrayList<>();
    // DanteTalkingPoint dtp = new DanteTalkingPoint();
    // dtp.setCustomerID("test");
    // dtp.setExternalID(externalID);
    // dtp.setPlayExternalID("testDPlayExtID");
    // dtp.setValue("Deployment Test Talking Point");
    // dtps.add(dtp);
    //
    // ResponseDocument result = talkingPointProxy.createOrUpdate(dtps);
    // Assert.assertNull(result.getErrors());
    //
    // dtp = danteTalkingPointEntityMgr.findByExternalID(dtp.getExternalID());
    //
    // Assert.assertNotNull(dtp.getCreationDate(), "Failure Cause: Creation Date
    // is NULL");
    // Assert.assertNotNull(dtp.getLastModificationDate(), "Failure Cause:
    // LastModificationDate is NULL");
    //
    // Date oldLastModificationDate = dtp.getLastModificationDate();
    // dtp.setValue("New Deployment Test Talking Point");
    // dtps = new ArrayList<>();
    // dtps.add(dtp);
    //
    // talkingPointProxy.createOrUpdate(dtps);
    // dtp =
    // objMapper.convertValue(talkingPointProxy.findByExternalID(externalID).getResult(),
    // new TypeReference<DanteTalkingPoint>() {
    // });
    //
    // Assert.assertNotNull(dtp,
    // "Failure Cause: Talking Point not found by extrenal ID where externalID =
    // " + externalID);
    // Assert.assertEquals(dtp.getValue(), "New Deployment Test Talking Point",
    // "Failure Cause: Talking Point value incorrect");
    //
    // Assert.assertNotEquals(dtp.getLastModificationDate(),
    // oldLastModificationDate,
    // "Failure Cause: Lastmodification date not updated by createOrUpdate()");
    //
    // dtps =
    // objMapper.convertValue(talkingPointProxy.findAllByPlayID("testDPlayExtID").getResult(),
    // new TypeReference<List<DanteTalkingPoint>>() {
    // });
    //
    // Assert.assertEquals(dtps.size(), 1, "Failure Cause: Talking Points not
    // found by findByPlayID");
    //
    // talkingPointProxy.delete(dtp.getExternalID());
    //
    // dtp = danteTalkingPointEntityMgr.findByField("External_ID", externalID);
    // Assert.assertNull(dtp, "Failure Cause: Talking point was not deleted");
    // }
    //
    // @Test(groups = "deployment")
    // public void testDanteOauth() {
    // String testTenantName = "OauthTest.OauthTest.Production";
    // ResponseDocument<DantePreviewResources> response =
    // talkingPointProxy.getPreviewResources(testTenantName);
    //
    // Assert.assertNotNull(response);
    // Assert.assertNull(response.getErrors());
    // Assert.assertNotNull(response.getResult());
    //
    // DantePreviewResources dpr = objMapper.convertValue(response.getResult(),
    // new TypeReference<DantePreviewResources>() {
    // });
    // String url = dpr.getServerUrl() + "/tenants/oauthtotenant";
    // RestTemplate restTemplate = HttpClientUtils.newRestTemplate();
    // HttpHeaders headers = new HttpHeaders();
    // headers.add("Authorization", "Bearer " + dpr.getoAuthToken());
    // HttpEntity<String> entity = new HttpEntity<>("parameters", headers);
    //
    // String tenantNameViaToken = restTemplate.exchange(url, HttpMethod.GET,
    // entity, String.class).getBody();
    // Assert.assertNotNull(tenantNameViaToken);
    // Assert.assertEquals(tenantNameViaToken, testTenantName);
    // }
}
