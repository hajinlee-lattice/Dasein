package com.latticeengines.playmaker.entitymgr;

import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.mockito.MockitoAnnotations;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.support.DirtiesContextTestExecutionListener;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Listeners;
import org.testng.annotations.Test;

import com.latticeengines.db.exposed.entitymgr.TenantEntityMgr;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.CDLConstants;
import com.latticeengines.domain.exposed.playmaker.PlaymakerConstants;
import com.latticeengines.domain.exposed.playmaker.PlaymakerSyncLookupSource;
import com.latticeengines.domain.exposed.playmakercore.Recommendation;
import com.latticeengines.domain.exposed.playmakercore.SynchronizationDestinationEnum;
import com.latticeengines.domain.exposed.pls.LaunchState;
import com.latticeengines.domain.exposed.pls.Play;
import com.latticeengines.domain.exposed.pls.PlayLaunch;
import com.latticeengines.domain.exposed.pls.RatingBucketName;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.playmakercore.entitymanager.RecommendationEntityMgr;
import com.latticeengines.proxy.exposed.cdl.PlayProxy;
import com.latticeengines.testframework.service.impl.GlobalAuthCleanupTestListener;
import com.latticeengines.testframework.service.impl.TestPlayCreationHelper;

@Listeners({ GlobalAuthCleanupTestListener.class })
@TestExecutionListeners({ DirtiesContextTestExecutionListener.class })
@ContextConfiguration(locations = { //
        "classpath:test-playmaker-context.xml", "classpath:proxy-context.xml",
        "classpath:test-testframework-cleanup-context.xml" })
public class PlaymakerRecommendationEntityMgrV2DeploymentTestNG extends AbstractTestNGSpringContextTests {

//    @Value("${common.test.pls.url}")
//    private String internalResourceHostPort;
//
//    @Inject
//    private PlaymakerRecommendationEntityMgr playmakerRecommendationMgr;
//    
//    @Inject
//    private PlayProxy playProxy;
//
//    private Tenant tenant;
//
//    private Play play;
//
//    private PlayLaunch playLaunch;
//
//    @Value("${datadb.datasource.driver}")
//    private String dataDbDriver;
//
//    @Value("${datadb.datasource.sqoop.url}")
//    private String dataDbUrl;
//
//    @Value("${datadb.datasource.user}")
//    private String dataDbUser;
//
//    @Value("${datadb.datasource.password.encrypted}")
//    private String dataDbPassword;
//
//    @Value("${datadb.datasource.dialect}")
//    private String dataDbDialect;
//
//    @Value("${datadb.datasource.type}")
//    private String dataDbType;
//
//    @Inject
//    private RecommendationEntityMgr recommendationEntityMgr;
//
//    @Inject
//    TenantEntityMgr tenantEntityMgr;
//
//    @Inject
//    private TestPlayCreationHelper testPlayCreationHelper;
//
//    String randId = UUID.randomUUID().toString();
//
//    private CustomerSpace customerSpace;
//
//    private Set<RatingBucketName> bucketsToLaunch;
//
//    private Boolean excludeItemsWithoutSalesforceId;
//
//    private Long topNCount;
//
//    private int maxUpdateRows = 20;
//    private String syncDestination = "SFDC";
//    private Map<String, String> orgInfo;
//    private Map<String, String> badOrgInfo;
//    private Map<String, String> eloquaAppId;

    @BeforeClass(groups = "deployment")
    public void setup() throws Exception {
//        eloquaAppId = new HashMap<String, String>();
//        eloquaAppId.put(CDLConstants.AUTH_APP_ID, "BIS01234");
//
//        bucketsToLaunch = new HashSet<>(Arrays.asList(RatingBucketName.values()));
//        excludeItemsWithoutSalesforceId = true;
//        topNCount = 5L;
//
//        testPlayCreationHelper.setupTenantAndData();
////        testPlayCreationHelper.setupPlayTestEnv();
////        testPlayCreationHelper.createPlay();
////        testPlayCreationHelper.createPlayLaunch(true, bucketsToLaunch, excludeItemsWithoutSalesforceId, topNCount);
//        
//        testPlayCreationHelper.setupTenantAndCreatePlay();
//        
//        tenant = testPlayCreationHelper.getTenant();
//
//        MockitoAnnotations.initMocks(this);
//
//        customerSpace = CustomerSpace.parse(tenant.getId());
//
//        tenant = testPlayCreationHelper.getTenant();
//        play = testPlayCreationHelper.getPlay();
//        playLaunch = testPlayCreationHelper.getPlayLaunch();
//        
//        playProxy.updatePlayLaunch(MultiTenantContext.getCustomerSpace().toString(), play.getName(),
//                playLaunch.getLaunchId(), LaunchState.Launched);
//
//        List<Recommendation> recommendations = recommendationEntityMgr.findAll();
//        Assert.assertTrue(CollectionUtils.isEmpty(recommendations));
//
//        orgInfo = testPlayCreationHelper.getOrgInfo();;
////        orgInfo.put(CDLConstants.ORG_ID, testPlayCreationHelper.getDestinationOrgId());
////        orgInfo.put(CDLConstants.EXTERNAL_SYSTEM_TYPE, testPlayCreationHelper.getDestinationOrgType().name());
//
//        badOrgInfo = new HashMap<>();
//        badOrgInfo.put(CDLConstants.ORG_ID, "BAD_ID_" + System.currentTimeMillis());
//        badOrgInfo.put(CDLConstants.EXTERNAL_SYSTEM_TYPE, "CRM");
//
//        createDummyRecommendations(maxUpdateRows * 2, new Date());
    }

    @AfterClass(groups = { "deployment" })
    public void teardown() throws Exception {
        //testPlayCreationHelper.cleanupArtifacts();
    }

//    @Test(groups = "deployment")
//    public void testPlays() {
//        Map<String, Object> playCount = playmakerRecommendationMgr.getPlayCount(customerSpace.toString(),
//                PlaymakerSyncLookupSource.V2.name(), 0, null, SynchronizationDestinationEnum.SFDC.ordinal(),
//                badOrgInfo);
//        Assert.assertNotNull(playCount);
//        Object countObj = playCount.get(PlaymakerRecommendationEntityMgr.COUNT_KEY);
//        Assert.assertNotNull(countObj);
//        Long count = (Long) countObj;
//        Assert.assertNotNull(count);
//        Assert.assertTrue(count == 0);
//
//        playCount = playmakerRecommendationMgr.getPlayCount(customerSpace.toString(),
//                PlaymakerSyncLookupSource.V2.name(), 0, null, SynchronizationDestinationEnum.SFDC.ordinal(), orgInfo);
//        Assert.assertNotNull(playCount);
//        countObj = playCount.get(PlaymakerRecommendationEntityMgr.COUNT_KEY);
//        Assert.assertNotNull(countObj);
//        count = (Long) countObj;
//        Assert.assertNotNull(count);
//        Assert.assertTrue(count > 0);
//        // not actual restriction - only for this test scenario
//        Assert.assertTrue(count < 100);
//
//        Map<String, Object> plays = playmakerRecommendationMgr.getPlays(customerSpace.toString(),
//                PlaymakerSyncLookupSource.V2.name(), 0, 0, 10, null, SynchronizationDestinationEnum.SFDC.ordinal(),
//                orgInfo);
//        Assert.assertNotNull(plays);
//        Assert.assertNotNull(plays.get(PlaymakerRecommendationEntityMgr.START_KEY));
//        Assert.assertNotNull(plays.get(PlaymakerRecommendationEntityMgr.END_KEY));
//        Assert.assertNotNull(plays.get(PlaymakerRecommendationEntityMgr.RECORDS_KEY));
//        @SuppressWarnings({ "unchecked" })
//        List<Map<String, Object>> result = (List<Map<String, Object>>) plays
//                .get(PlaymakerRecommendationEntityMgr.RECORDS_KEY);
//        Assert.assertNotNull(result);
//        Assert.assertEquals(result.size(), count.intValue());
//        Assert.assertTrue(CollectionUtils.isNotEmpty(result));
//        result.stream() //
//                .forEach(playMap -> {
//                    Assert.assertNotNull(playMap);
//                    Assert.assertTrue(MapUtils.isNotEmpty(playMap));
//                    Assert.assertNotNull(playMap.get(PlaymakerConstants.ID));
//                    Assert.assertNotNull(playMap.get(PlaymakerConstants.ID + PlaymakerConstants.V2));
//                    Assert.assertNotNull(playMap.get(PlaymakerConstants.ExternalId));
//                    Assert.assertNotNull(playMap.get(PlaymakerConstants.DisplayName));
//                    Assert.assertNotNull(playMap.get(PlaymakerConstants.RowNum));
//                });
//    }
//
//    @Test(groups = "deployment")
//    public void testRecommendation() {
//        Map<String, Object> recommendations = playmakerRecommendationMgr.getRecommendations(customerSpace.toString(),
//                PlaymakerSyncLookupSource.V2.name(), 0L, 0, 100, SynchronizationDestinationEnum.SFDC.ordinal(), null,
//                orgInfo, eloquaAppId);
//        Assert.assertNotNull(recommendations);
//        Assert.assertNotNull(recommendations.get(PlaymakerRecommendationEntityMgr.START_KEY));
//        Assert.assertNotNull(recommendations.get(PlaymakerRecommendationEntityMgr.END_KEY));
//        Assert.assertNotNull(recommendations.get(PlaymakerRecommendationEntityMgr.RECORDS_KEY));
//
//        @SuppressWarnings({ "unchecked" })
//        List<Map<String, Object>> result = (List<Map<String, Object>>) recommendations
//                .get(PlaymakerRecommendationEntityMgr.RECORDS_KEY);
//        Assert.assertNotNull(result);
//        Assert.assertEquals(result.size(), maxUpdateRows * 2);
//        Assert.assertTrue(CollectionUtils.isNotEmpty(result));
//        result.stream() //
//                .forEach(recMap -> {
//                    Assert.assertNotNull(recMap);
//                    Assert.assertTrue(MapUtils.isNotEmpty(recMap));
//                    Assert.assertNotNull(recMap.get(PlaymakerConstants.ID));
//                    Assert.assertNotNull(recMap.get(PlaymakerConstants.PlayID + PlaymakerConstants.V2));
//                    Assert.assertNotNull(recMap.get(PlaymakerConstants.LaunchID + PlaymakerConstants.V2));
//                    Assert.assertNotNull(recMap.get(PlaymakerConstants.LEAccountExternalID));
//                    Assert.assertNotNull(recMap.get(PlaymakerConstants.Description));
//                });
//    }
//
//    private void createDummyRecommendations(int newRecommendationsCount, Date launchDate) {
//        while (newRecommendationsCount-- > 0) {
//            Recommendation rec = new Recommendation();
//            rec.setAccountId("Acc_" + launchDate.toInstant().toEpochMilli() + "_" + newRecommendationsCount);
//            rec.setCompanyName("CN_" + launchDate.toInstant().toEpochMilli() + "_" + newRecommendationsCount);
//            rec.setDeleted(false);
//            rec.setDestinationOrgId(orgInfo.get(CDLConstants.ORG_ID));
//            rec.setDestinationSysType(orgInfo.get(CDLConstants.EXTERNAL_SYSTEM_TYPE));
//            rec.setId("ID_" + launchDate.toInstant().toEpochMilli() + "_" + newRecommendationsCount);
//            rec.setLaunchDate(launchDate);
//            rec.setLaunchId(playLaunch.getId());
//            rec.setLeAccountExternalID("Acc_" + launchDate.toInstant().toEpochMilli() + "_" + newRecommendationsCount);
//            rec.setPlayId(play.getName());
//            rec.setRecommendationId("ID_" + launchDate.toInstant().toEpochMilli() + "_" + newRecommendationsCount);
//            rec.setSynchronizationDestination(syncDestination);
//            rec.setTenantId(tenant.getPid());
//            String contactStr = "[{\"Email\":\"FirstName5763@com\",  \"Address\": \"null Dr\",  \"Phone\":\"248.813.2000\",\"State\":\"MI\",\"ZipCode\":\"48098-2815\","
//                    + "\"ContactID\":\"" + String.valueOf(launchDate.toInstant().toEpochMilli()) + "\","
//                    + "\"Country\":\"USA\",\"SfdcContactID\": \"\",\"City\": \"Troy\",\"ContactID\": \"5763\",\"Name\": \"FirstName5763 LastName5763\"}]";
//            rec.setContacts(contactStr);
//            recommendationEntityMgr.create(rec);
//        }
//    }
}
