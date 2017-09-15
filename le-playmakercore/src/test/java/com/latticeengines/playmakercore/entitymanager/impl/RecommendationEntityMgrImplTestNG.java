package com.latticeengines.playmakercore.entitymanager.impl;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.support.DirtiesContextTestExecutionListener;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.playmaker.PlaymakerConstants;
import com.latticeengines.domain.exposed.playmaker.PlaymakerUtils;
import com.latticeengines.domain.exposed.playmakercore.Recommendation;
import com.latticeengines.domain.exposed.playmakercore.SynchronizationDestinationEnum;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.playmakercore.entitymanager.RecommendationEntityMgr;
import com.latticeengines.security.exposed.util.MultiTenantContext;

@TestExecutionListeners({ DirtiesContextTestExecutionListener.class })
@ContextConfiguration(locations = { "classpath:test-playmakercore-context.xml" })
public class RecommendationEntityMgrImplTestNG extends AbstractTestNGSpringContextTests {

    @Autowired
    private RecommendationEntityMgr recommendationEntityMgr;

    private Recommendation recommendation;

    private long CURRENT_TIME_MILLIS = System.currentTimeMillis();

    private Date CURRENT_DATE = new Date(System.currentTimeMillis());

    private String PLAY_ID = "play__" + CURRENT_TIME_MILLIS;
    private String LAUNCH_ID = "launch__" + CURRENT_TIME_MILLIS;
    private String ACCOUNT_ID = "account__" + CURRENT_TIME_MILLIS;
    private String LAUNCH_DESCRIPTION = "Recommendation done on " + CURRENT_TIME_MILLIS;
    private long TENANT_PID = 1L;
    private String CUSTOMER_SPACE = "LocalTest.LocalTest.Production";
    private String DUMMY_EMAIL = "FirstName5763@com";
    private String DUMMY_ZIP = "48098-2815";

    private Tenant tenant;

    @BeforeClass(groups = "functional")
    public void setup() throws Exception {
        recommendation = new Recommendation();
        recommendation.setDescription(LAUNCH_DESCRIPTION);
        recommendation.setLaunchId(LAUNCH_ID);
        recommendation.setLaunchDate(CURRENT_DATE);
        recommendation.setPlayId(PLAY_ID);
        recommendation.setAccountId(ACCOUNT_ID);
        recommendation.setLeAccountExternalID(ACCOUNT_ID);
        recommendation.setTenantId(TENANT_PID);
        String contacts = //
                " [ " //
                        + "  { " //
                        + "   \"Email\": \"" + DUMMY_EMAIL + "\", " //
                        + "   \"Address\": \"null Dr\", " //
                        + "   \"Phone\": \"248.813.2000\", " //
                        + "   \"State\": \"MI\", " //
                        + "   \"ZipCode\": \"" + DUMMY_ZIP + "\", " //
                        + "   \"Country\": \"USA\", " //
                        + "   \"SfdcContactID\": \"\", " //
                        + "   \"City\": \"Troy\", " //
                        + "   \"ContactID\": \"5763\", " //
                        + "   \"Name\": \"FirstName5763 LastName5763\" " //
                        + "  } " //
                        + " ] ";
        recommendation.setContacts(contacts);
        recommendation.setSynchronizationDestination(SynchronizationDestinationEnum.SFDC.toString());

        tenant = new Tenant(CUSTOMER_SPACE);

        MultiTenantContext.setTenant(tenant);
    }

    @Test(groups = "functional")
    public void testGetPreCreate() {
    }

    @Test(groups = "functional", dependsOnMethods = { "testGetPreCreate" })
    public void testCreateRecommendation() {
        recommendationEntityMgr.create(recommendation);
        Assert.assertNotNull(recommendation.getRecommendationId());
        Assert.assertNotNull(recommendation.getPid());
    }

    @Test(groups = "functional", dependsOnMethods = { "testCreateRecommendation" })
    public void testGetRecommendationByLaunchId() {

        tenant = new Tenant(CUSTOMER_SPACE);
        MultiTenantContext.setTenant(tenant);

        List<Recommendation> recommendations = recommendationEntityMgr.findByLaunchId(LAUNCH_ID);
        Assert.assertNotNull(recommendations);
        Assert.assertEquals(recommendations.size(), 1);

        for (Recommendation recommendation : recommendations) {
            Assert.assertNotNull(recommendation.getRecommendationId());
            Assert.assertNotNull(recommendation.getPid());
            Assert.assertNotNull(recommendation.getContacts());
            List<Map<String, String>> contactList = recommendation.getExpandedContacts();
            Assert.assertNotNull(contactList);
            Assert.assertTrue(contactList.size() == 1);
            Assert.assertEquals(contactList.get(0).get("Email"), DUMMY_EMAIL);
            Assert.assertEquals(contactList.get(0).get("ZipCode"), DUMMY_ZIP);
        }
    }

    @Test(groups = "functional", dependsOnMethods = { "testGetRecommendationByLaunchId" })
    public void testGetRecommendationByPlayId() {

        tenant = new Tenant(CUSTOMER_SPACE);
        MultiTenantContext.setTenant(tenant);

        List<String> playIds = new ArrayList<>();
        playIds.add(PLAY_ID);

        int recommendationCount = recommendationEntityMgr.findRecommendationCount(new Date(0L), //
                SynchronizationDestinationEnum.SFDC.toString(), playIds);

        Assert.assertEquals(1, recommendationCount);

        List<Recommendation> recommendations = recommendationEntityMgr.findRecommendations(new Date(0L), //
                0, recommendationCount, SynchronizationDestinationEnum.SFDC.toString(), playIds);
        Assert.assertNotNull(recommendations);
        Assert.assertTrue(recommendations.size() > 0);
        Assert.assertEquals(recommendations.size(), recommendationCount);

        for (Recommendation recommendation : recommendations) {
            Assert.assertNotNull(recommendation.getRecommendationId());
            Assert.assertNotNull(recommendation.getPid());
        }
    }

    @Test(groups = "functional", dependsOnMethods = { "testGetRecommendationByPlayId" })
    public void testGetRecommendationAsMapByPlayId() {

        tenant = new Tenant(CUSTOMER_SPACE);
        MultiTenantContext.setTenant(tenant);

        List<String> playIds = new ArrayList<>();
        playIds.add(PLAY_ID);

        int recommendationCount = recommendationEntityMgr.findRecommendationCount(new Date(0L), //
                SynchronizationDestinationEnum.SFDC.toString(), playIds);

        Assert.assertEquals(1, recommendationCount);

        List<Map<String, Object>> recommendations = recommendationEntityMgr.findRecommendationsAsMap(new Date(0L), //
                0, recommendationCount, SynchronizationDestinationEnum.SFDC.toString(), playIds);
        Assert.assertNotNull(recommendations);
        Assert.assertTrue(recommendations.size() > 0);
        Assert.assertEquals(recommendations.size(), recommendationCount);

        Long lastModificationDate = 0L;

        for (Map<String, Object> recommendation : recommendations) {
            Assert.assertTrue(recommendation.size() > 0);
            Object timestamp = recommendation.get(PlaymakerConstants.LastModificationDate);
            if (!(timestamp instanceof Long)) {
                timestamp = Long.parseLong(timestamp.toString());
            }
            if ((Long) timestamp > lastModificationDate) {
                lastModificationDate = (Long) timestamp;
            }
        }

        Date lastModificationDate2 = PlaymakerUtils.dateFromEpochSeconds(lastModificationDate + 1);
        Date lastModificationDate3 = PlaymakerUtils.dateFromEpochSeconds(lastModificationDate - 1);
        recommendationCount = recommendationEntityMgr.findRecommendationCount(lastModificationDate2, //
                SynchronizationDestinationEnum.SFDC.toString(), playIds);
        Assert.assertEquals(0, recommendationCount);

        recommendations = recommendationEntityMgr.findRecommendationsAsMap(lastModificationDate2, //
                0, recommendationCount, SynchronizationDestinationEnum.SFDC.toString(), playIds);
        Assert.assertNotNull(recommendations);
        Assert.assertTrue(recommendations.size() == 0);

        recommendations = recommendationEntityMgr.findRecommendationsAsMap(lastModificationDate3, //
                0, recommendationCount, SynchronizationDestinationEnum.SFDC.toString(), playIds);
        Assert.assertNotNull(recommendations);
        Assert.assertTrue(recommendations.size() > 0);

        recommendationCount = recommendationEntityMgr.findRecommendationCount(lastModificationDate3, //
                SynchronizationDestinationEnum.SFDC.toString(), playIds);
        Assert.assertTrue(recommendationCount > 0);
    }

    @Test(groups = "functional", dependsOnMethods = { "testGetRecommendationAsMapByPlayId" })
    public void testGetRecommendationWithoutPlayId() {

        tenant = new Tenant(CUSTOMER_SPACE);
        MultiTenantContext.setTenant(tenant);

        int recommendationCount = recommendationEntityMgr.findRecommendationCount(new Date(0L), //
                SynchronizationDestinationEnum.SFDC.toString(), null);

        Assert.assertTrue(recommendationCount > 0);

        int minPageSize = Math.min(10, recommendationCount);

        List<Recommendation> recommendations = recommendationEntityMgr.findRecommendations(new Date(0L), //
                (recommendationCount - minPageSize), minPageSize, SynchronizationDestinationEnum.SFDC.toString(), null);
        Assert.assertNotNull(recommendations);
        Assert.assertTrue(recommendations.size() > 0);
        Assert.assertEquals(recommendations.size(), minPageSize);
        Long lastModificationDate = 0L;

        for (Recommendation recommendation : recommendations) {
            Assert.assertNotNull(recommendation.getRecommendationId());
            Assert.assertNotNull(recommendation.getPid());
            Assert.assertNotNull(recommendation.getTenantId());
            Assert.assertEquals(recommendation.getTenantId().longValue(), TENANT_PID);
            Object timestamp = new Long(recommendation.getLastUpdatedTimestamp().getTime());
            if (!(timestamp instanceof Long)) {
                timestamp = Long.parseLong(timestamp.toString());
            }
            if ((Long) timestamp > lastModificationDate) {
                lastModificationDate = (Long) timestamp;
            }
        }
        lastModificationDate = lastModificationDate / 1000;

        Date lastModificationDate2 = PlaymakerUtils.dateFromEpochSeconds(lastModificationDate + 1);
        Date lastModificationDate3 = PlaymakerUtils.dateFromEpochSeconds(lastModificationDate - 1);
        recommendationCount = recommendationEntityMgr.findRecommendationCount(lastModificationDate2, //
                SynchronizationDestinationEnum.SFDC.toString(), null);

        Assert.assertEquals(0, recommendationCount);

        recommendations = recommendationEntityMgr.findRecommendations(lastModificationDate2, //
                (recommendationCount - minPageSize), minPageSize, SynchronizationDestinationEnum.SFDC.toString(), null);
        Assert.assertNotNull(recommendations);
        Assert.assertTrue(recommendations.size() == 0);

        recommendations = recommendationEntityMgr.findRecommendations(lastModificationDate3, //
                (recommendationCount - minPageSize), minPageSize, SynchronizationDestinationEnum.SFDC.toString(), null);
        Assert.assertNotNull(recommendations);
        Assert.assertTrue(recommendations.size() > 0);

        recommendationCount = recommendationEntityMgr.findRecommendationCount(lastModificationDate3, //
                SynchronizationDestinationEnum.SFDC.toString(), null);
        Assert.assertTrue(recommendationCount > 0);
    }

    @AfterClass(groups = "functional")
    public void teardown() throws Exception {
    }
}
