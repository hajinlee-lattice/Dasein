package com.latticeengines.playmakercore.entitymanager.impl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.support.DirtiesContextTestExecutionListener;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.cdl.CDLConstants;
import com.latticeengines.domain.exposed.playmaker.PlaymakerConstants;
import com.latticeengines.domain.exposed.playmaker.PlaymakerUtils;
import com.latticeengines.domain.exposed.playmakercore.Recommendation;
import com.latticeengines.domain.exposed.playmakercore.SynchronizationDestinationEnum;
import com.latticeengines.domain.exposed.pls.RatingBucketName;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.playmakercore.entitymanager.RecommendationEntityMgr;

@TestExecutionListeners({ DirtiesContextTestExecutionListener.class })
@ContextConfiguration(locations = { "classpath:test-playmakercore-context.xml" })
public class RecommendationEntityMgrImplTestNG extends AbstractTestNGSpringContextTests {

    @Autowired
    private RecommendationEntityMgr recommendationEntityMgr;

    private Recommendation recommendationWithoutOrgInfo;
    private Recommendation recommendationWithOrg1;
    private Recommendation recommendationWithOrg2;

    private long CURRENT_TIME_MILLIS = System.currentTimeMillis();

    private Date CURRENT_DATE = new Date(System.currentTimeMillis());

    private String PLAY_ID = "play__" + CURRENT_TIME_MILLIS;
    private String LAUNCH_ID = "launch__" + CURRENT_TIME_MILLIS;
    private String LAUNCH_ID_1 = "launch1__" + CURRENT_TIME_MILLIS;
    private String LAUNCH_ID_2 = "launch2__" + CURRENT_TIME_MILLIS;
    private String ACCOUNT_ID = "account__" + CURRENT_TIME_MILLIS;
    private String SFDC_ACCOUNT_ID = "sfdcAcc__" + CURRENT_TIME_MILLIS;
    private String SFDC_ACCOUNT_ID_1 = "sfdcAcc1__" + CURRENT_TIME_MILLIS;
    private String SFDC_ACCOUNT_ID_2 = "sfdcAcc2__" + CURRENT_TIME_MILLIS;
    private String LAUNCH_DESCRIPTION = "Recommendation done on " + CURRENT_TIME_MILLIS;
    private long TENANT_PID = CURRENT_TIME_MILLIS;
    private String CUSTOMER_SPACE = "LocalTest.LocalTest.Production";
    private String DUMMY_EMAIL = "FirstName5763@com";
    private String DUMMY_ZIP = "48098-2815";
    private String DESTINATION_SYS_TYPE_1 = "destinationSysType_1";
    private String DESTINATION_ORG_ID_1 = "destinationOrgId_1";
    private String DESTINATION_SYS_TYPE_2 = "destinationSysType_2";
    private String DESTINATION_ORG_ID_2 = "destinationOrgId_2";

    private List<Recommendation> allRecommendationsAcrossAllLaunches = null;

    private Tenant tenant;

    @BeforeClass(groups = "functional")
    public void setup() throws Exception {

        recommendationWithoutOrgInfo = createRecommendationObject(LAUNCH_ID, SFDC_ACCOUNT_ID, null, null, "A");
        recommendationWithOrg1 = createRecommendationObject(LAUNCH_ID_1, SFDC_ACCOUNT_ID_1, DESTINATION_ORG_ID_1,
                DESTINATION_SYS_TYPE_1, "B");
        recommendationWithOrg2 = createRecommendationObject(LAUNCH_ID_2, SFDC_ACCOUNT_ID_2, DESTINATION_ORG_ID_2,
                DESTINATION_SYS_TYPE_2, "C");

        allRecommendationsAcrossAllLaunches = Arrays.asList(recommendationWithoutOrgInfo, recommendationWithOrg1,
                recommendationWithOrg2);
        tenant = new Tenant(CUSTOMER_SPACE);
        tenant.setPid(TENANT_PID);
        MultiTenantContext.setTenant(tenant);
    }

    @AfterClass(groups = "functional")
    public void teardown() {
        allRecommendationsAcrossAllLaunches.stream().forEach(rec -> {
            try {
                if (rec.getPid() != null) {
                    recommendationEntityMgr.delete(rec);
                }
            } catch (Exception ex) {
                // ignore error in cleanup
            }
        });
    }

    @Test(groups = "functional")
    public void testCreateRecommendation() {
        allRecommendationsAcrossAllLaunches.stream()//
                .forEach(rec -> testCreateRecommendation(rec));
    }

    @Test(groups = "functional", dependsOnMethods = { "testCreateRecommendation" })
    public void testGetRecommendationById() throws InterruptedException {
        allRecommendationsAcrossAllLaunches.stream()//
                .forEach(rec -> testGetRecommendationById(rec));
    }

    @Test(groups = "functional", dependsOnMethods = { "testCreateRecommendation" })
    public void testGetRecommendationByLaunchId() {
        allRecommendationsAcrossAllLaunches.stream()//
                .forEach(rec -> testGetRecommendationByLaunchId(rec));
    }

    @Test(groups = "functional", dependsOnMethods = { "testGetRecommendationByLaunchId" })
    public void testGetRecommendationByPlayId() {
        allRecommendationsAcrossAllLaunches.stream()//
                .forEach(rec -> testGetRecommendationByPlayId(rec));
    }

    @Test(groups = "functional", dependsOnMethods = { "testGetRecommendationByPlayId" })
    public void testGetRecommendationAsMapByPlayId() {
        allRecommendationsAcrossAllLaunches.stream()//
                .forEach(rec -> testGetRecommendationAsMapByPlayId(rec));
    }

    @Test(groups = "functional", dependsOnMethods = { "testGetRecommendationAsMapByPlayId" })
    public void testGetRecommendationWithoutPlayId() {
        allRecommendationsAcrossAllLaunches.stream()//
                .forEach(rec -> testGetRecommendationWithoutPlayId(rec));
    }

    private Recommendation createRecommendationObject(String launchId, String sfdcAccountID, String destinationOrgId,
            String destinationSysType, String priorityDisplayName) {
        Recommendation recommendation = new Recommendation();
        recommendation.setDescription(LAUNCH_DESCRIPTION);
        recommendation.setLaunchId(launchId);
        recommendation.setLaunchDate(CURRENT_DATE);
        recommendation.setPlayId(PLAY_ID);
        recommendation.setAccountId(ACCOUNT_ID);
        recommendation.setSfdcAccountID(sfdcAccountID);
        recommendation.setLeAccountExternalID(ACCOUNT_ID);
        recommendation.setPriorityDisplayName(priorityDisplayName);
        recommendation.setPriorityID(RatingBucketName.valueOf(priorityDisplayName));
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
        recommendation.setDestinationOrgId(destinationOrgId);
        recommendation.setDestinationSysType(destinationSysType);
        return recommendation;
    }

    private void testCreateRecommendation(Recommendation recommendation) {
        Assert.assertNotNull(recommendation.getPriorityDisplayName());
        Assert.assertNotNull(recommendation.getPriorityID());
        recommendationEntityMgr.create(recommendation);
        Assert.assertNotNull(recommendation.getRecommendationId());
        Assert.assertNotNull(recommendation.getPid());
        Assert.assertNotNull(recommendation.getPriorityDisplayName());
        Assert.assertNotNull(recommendation.getPriorityID());

    }

    private void testGetRecommendationById(Recommendation originalRec) {
        MultiTenantContext.setTenant(tenant);
        try {
            Thread.sleep(500);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        // wait for replication lag
        Recommendation recommendation = recommendationEntityMgr
                .findByRecommendationId(originalRec.getRecommendationId());
        compareOriginalAndExtractedRecommendations(originalRec, recommendation);
    }

    private void testGetRecommendationByLaunchId(Recommendation originalRec) {
        MultiTenantContext.setTenant(tenant);

        List<Recommendation> recommendations = recommendationEntityMgr.findByLaunchId(originalRec.getLaunchId());
        Assert.assertNotNull(recommendations);
        Assert.assertEquals(recommendations.size(), 1);

        for (Recommendation recommendation : recommendations) {
            compareOriginalAndExtractedRecommendations(originalRec, recommendation);
        }
    }

    private void testGetRecommendationByPlayId(Recommendation originalRecommendation) {
        MultiTenantContext.setTenant(tenant);

        List<String> playIds = new ArrayList<>();
        playIds.add(originalRecommendation.getPlayId());
        Map<String, String> orgInfo = null;
        if (StringUtils.isNotBlank(originalRecommendation.getDestinationOrgId())) {
            orgInfo = new HashMap<>();
            orgInfo.put(CDLConstants.ORG_ID, originalRecommendation.getDestinationOrgId());
            orgInfo.put(CDLConstants.EXTERNAL_SYSTEM_TYPE, originalRecommendation.getDestinationSysType());
        }

        int recommendationCount = recommendationEntityMgr.findRecommendationCount(new Date(0L), //
                SynchronizationDestinationEnum.SFDC.toString(), playIds, orgInfo);

        Assert.assertEquals(1, recommendationCount);

        List<Recommendation> recommendations = recommendationEntityMgr.findRecommendations(new Date(0L), //
                0, recommendationCount, SynchronizationDestinationEnum.SFDC.toString(), playIds, orgInfo);

        Assert.assertNotNull(recommendations);
        Assert.assertTrue(recommendations.size() > 0);
        Assert.assertEquals(recommendations.size(), recommendationCount);

        for (Recommendation recommendation : recommendations) {
            compareOriginalAndExtractedRecommendations(originalRecommendation, recommendation);
        }
    }

    private void testGetRecommendationAsMapByPlayId(Recommendation originalRecommendation) {
        MultiTenantContext.setTenant(tenant);

        List<String> playIds = new ArrayList<>();
        playIds.add(originalRecommendation.getPlayId());
        Map<String, String> orgInfo = null;
        if (StringUtils.isNotBlank(originalRecommendation.getDestinationOrgId())) {
            orgInfo = new HashMap<>();
            orgInfo.put(CDLConstants.ORG_ID, originalRecommendation.getDestinationOrgId());
            orgInfo.put(CDLConstants.EXTERNAL_SYSTEM_TYPE, originalRecommendation.getDestinationSysType());
        }

        int recommendationCount = recommendationEntityMgr.findRecommendationCount(new Date(0L), //
                SynchronizationDestinationEnum.SFDC.toString(), playIds, orgInfo);

        Assert.assertEquals(1, recommendationCount);

        List<Map<String, Object>> recommendations = recommendationEntityMgr.findRecommendationsAsMap(new Date(0L), //
                0, recommendationCount, SynchronizationDestinationEnum.SFDC.toString(), playIds, orgInfo);
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
            compareOriginalAndExtractedRecommendations(originalRecommendation, recommendation);
        }

        Date lastModificationDate2 = PlaymakerUtils.dateFromEpochSeconds(lastModificationDate + 1);
        Date lastModificationDate3 = PlaymakerUtils.dateFromEpochSeconds(lastModificationDate - 1);

        System.out.println("lastModificationDate = " + lastModificationDate);
        System.out.println("lastModificationDate2 = " + lastModificationDate2.getTime());
        System.out.println("lastModificationDate3 = " + lastModificationDate3.getTime());

        recommendationCount = recommendationEntityMgr.findRecommendationCount(lastModificationDate2, //
                SynchronizationDestinationEnum.SFDC.toString(), playIds, orgInfo);
        Assert.assertEquals(0, recommendationCount);

        recommendations = recommendationEntityMgr.findRecommendationsAsMap(lastModificationDate2, //
                0, recommendationCount, SynchronizationDestinationEnum.SFDC.toString(), playIds, orgInfo);
        Assert.assertNotNull(recommendations);
        Assert.assertEquals(recommendations.size(), recommendationCount);

        recommendationCount = recommendationEntityMgr.findRecommendationCount(lastModificationDate3, //
                SynchronizationDestinationEnum.SFDC.toString(), playIds, orgInfo);
        Assert.assertTrue(recommendationCount > 0);

        recommendations = recommendationEntityMgr.findRecommendationsAsMap(lastModificationDate3, //
                0, recommendationCount, SynchronizationDestinationEnum.SFDC.toString(), playIds, orgInfo);
        Assert.assertNotNull(recommendations);
        Assert.assertEquals(recommendations.size(), recommendationCount);
        compareOriginalAndExtractedRecommendations(originalRecommendation, recommendations.get(0));

    }

    private void testGetRecommendationWithoutPlayId(Recommendation originalRecommendation) {
        MultiTenantContext.setTenant(tenant);
        Map<String, String> orgInfo = null;
        if (StringUtils.isNotBlank(originalRecommendation.getDestinationOrgId())) {
            orgInfo = new HashMap<>();
            orgInfo.put(CDLConstants.ORG_ID, originalRecommendation.getDestinationOrgId());
            orgInfo.put(CDLConstants.EXTERNAL_SYSTEM_TYPE, originalRecommendation.getDestinationSysType());
        }

        int recommendationCount = recommendationEntityMgr.findRecommendationCount(new Date(0L), //
                SynchronizationDestinationEnum.SFDC.toString(), null, orgInfo);

        Assert.assertTrue(recommendationCount > 0);
        Assert.assertEquals(recommendationCount, 1);

        int minPageSize = Math.min(10, recommendationCount);

        List<Recommendation> recommendations = recommendationEntityMgr.findRecommendations(new Date(0L), //
                (recommendationCount - minPageSize), minPageSize, SynchronizationDestinationEnum.SFDC.toString(), null,
                orgInfo);
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
            compareOriginalAndExtractedRecommendations(originalRecommendation, recommendation);
        }
        lastModificationDate = lastModificationDate / 1000;

        Date lastModificationDate2 = PlaymakerUtils.dateFromEpochSeconds(lastModificationDate + 1);
        Date lastModificationDate3 = PlaymakerUtils.dateFromEpochSeconds(lastModificationDate - 1);

        System.out.println("lastModificationDate = " + lastModificationDate);
        System.out.println("lastModificationDate2 = " + lastModificationDate2.getTime());
        System.out.println("lastModificationDate3 = " + lastModificationDate3.getTime());

        recommendationCount = recommendationEntityMgr.findRecommendationCount(lastModificationDate2, //
                SynchronizationDestinationEnum.SFDC.toString(), null, orgInfo);

        Assert.assertEquals(0, recommendationCount);

        recommendations = recommendationEntityMgr.findRecommendations(lastModificationDate2, //
                (recommendationCount - minPageSize), minPageSize, SynchronizationDestinationEnum.SFDC.toString(), null,
                orgInfo);
        Assert.assertNotNull(recommendations);
        Assert.assertTrue(recommendations.size() == 0);

        recommendations = recommendationEntityMgr.findRecommendations(lastModificationDate3, //
                (recommendationCount - minPageSize), minPageSize, SynchronizationDestinationEnum.SFDC.toString(), null,
                orgInfo);
        Assert.assertNotNull(recommendations);
        compareOriginalAndExtractedRecommendations(originalRecommendation, recommendations.get(0));

        // TODO - enable it once fixed
        // Assert.assertTrue(recommendations.size() > 0);
        //
        // recommendationCount =
        // recommendationEntityMgr.findRecommendationCount(lastModificationDate3,
        // //
        // SynchronizationDestinationEnum.SFDC.toString(), null);
        // Assert.assertTrue(recommendationCount > 0);
    }

    private void compareOriginalAndExtractedRecommendations(Recommendation originalRec,
            Map<String, Object> recommendation) {
        Map<String, String> orgInfo = null;
        if (StringUtils.isNotBlank(originalRec.getDestinationOrgId())) {
            orgInfo = new HashMap<>();
            orgInfo.put(CDLConstants.ORG_ID, originalRec.getDestinationOrgId());
            orgInfo.put(CDLConstants.EXTERNAL_SYSTEM_TYPE, originalRec.getDestinationSysType());
        }

        try {

            Assert.assertNotNull(recommendation.get(PlaymakerConstants.ID));
            Assert.assertNotNull(recommendation.get(PlaymakerConstants.Contacts));
            Assert.assertEquals(recommendation.get(PlaymakerConstants.ID), originalRec.getRecommendationId());
            Assert.assertEquals(recommendation.get(PlaymakerConstants.AccountID), originalRec.getAccountId());
            Assert.assertEquals(recommendation.get(PlaymakerConstants.Description), originalRec.getDescription());
            Assert.assertEquals(recommendation.get(PlaymakerConstants.ID), originalRec.getId());
            Assert.assertEquals(recommendation.get(PlaymakerConstants.LaunchID), originalRec.getLaunchId());
            Assert.assertEquals(recommendation.get(PlaymakerConstants.LEAccountExternalID),
                    originalRec.getLeAccountExternalID());
            Assert.assertEquals(recommendation.get(PlaymakerConstants.MonetaryValueIso4217ID),
                    originalRec.getMonetaryValueIso4217ID());
            Assert.assertEquals(recommendation.get(PlaymakerConstants.PlayID), originalRec.getPlayId());
            Assert.assertEquals(recommendation.get(PlaymakerConstants.PriorityDisplayName),
                    originalRec.getPriorityDisplayName());
            Assert.assertEquals(recommendation.get(PlaymakerConstants.SfdcAccountID), originalRec.getSfdcAccountID());
            Assert.assertEquals(recommendation.get(PlaymakerConstants.Likelihood), originalRec.getLikelihood());
            Assert.assertEquals(recommendation.get(PlaymakerConstants.MonetaryValue), originalRec.getMonetaryValue());
            Assert.assertEquals(RatingBucketName.valueOf(recommendation.get(PlaymakerConstants.PriorityID).toString()),
                    originalRec.getPriorityID());
            List<Map<String, String>> contactList = PlaymakerUtils
                    .getExpandedContacts(recommendation.get(PlaymakerConstants.Contacts).toString());
            Assert.assertNotNull(contactList);
            Assert.assertTrue(contactList.size() == 1);
            Assert.assertEquals(contactList.get(0).get("Email"), DUMMY_EMAIL);
            Assert.assertEquals(contactList.get(0).get("ZipCode"), DUMMY_ZIP);
        } catch (AssertionError | Exception ex) {
            throw new RuntimeException(
                    String.format("Exception = %s, orgInfo = %s", ex.getMessage(), JsonUtils.serialize(orgInfo)), ex);
        }
    }

    private void compareOriginalAndExtractedRecommendations(Recommendation originalRec, Recommendation recommendation) {
        Map<String, String> orgInfo = null;
        if (StringUtils.isNotBlank(originalRec.getDestinationOrgId())) {
            orgInfo = new HashMap<>();
            orgInfo.put(CDLConstants.ORG_ID, originalRec.getDestinationOrgId());
            orgInfo.put(CDLConstants.EXTERNAL_SYSTEM_TYPE, originalRec.getDestinationSysType());
        }

        try {
            Assert.assertNotNull(recommendation.getRecommendationId());
            Assert.assertNotNull(recommendation.getPid());
            Assert.assertNotNull(recommendation.getContacts());
            Assert.assertEquals(recommendation.getRecommendationId(), originalRec.getRecommendationId());
            Assert.assertEquals(recommendation.getPid(), originalRec.getPid());
            Assert.assertEquals(recommendation.getAccountId(), originalRec.getAccountId());
            Assert.assertEquals(recommendation.getDescription(), originalRec.getDescription());
            Assert.assertEquals(recommendation.getDestinationOrgId(), originalRec.getDestinationOrgId());
            Assert.assertEquals(recommendation.getDestinationSysType(), originalRec.getDestinationSysType());
            Assert.assertEquals(recommendation.getId(), originalRec.getId());
            Assert.assertEquals(recommendation.getLaunchId(), originalRec.getLaunchId());
            Assert.assertEquals(recommendation.getLeAccountExternalID(), originalRec.getLeAccountExternalID());
            Assert.assertEquals(recommendation.getMonetaryValueIso4217ID(), originalRec.getMonetaryValueIso4217ID());
            Assert.assertEquals(recommendation.getPlayId(), originalRec.getPlayId());
            Assert.assertEquals(recommendation.getPriorityDisplayName(), originalRec.getPriorityDisplayName());
            Assert.assertEquals(recommendation.getSfdcAccountID(), originalRec.getSfdcAccountID());
            Assert.assertEquals(recommendation.getSynchronizationDestination(),
                    originalRec.getSynchronizationDestination());
            Assert.assertEquals(recommendation.getLikelihood(), originalRec.getLikelihood());
            Assert.assertEquals(recommendation.getMonetaryValue(), originalRec.getMonetaryValue());
            Assert.assertEquals(recommendation.getPriorityID(), originalRec.getPriorityID());
            Assert.assertEquals(recommendation.getTenantId(), originalRec.getTenantId());
            List<Map<String, String>> contactList = recommendation.getExpandedContacts();
            Assert.assertNotNull(contactList);
            Assert.assertTrue(contactList.size() == 1);
            Assert.assertEquals(contactList.get(0).get("Email"), DUMMY_EMAIL);
            Assert.assertEquals(contactList.get(0).get("ZipCode"), DUMMY_ZIP);
        } catch (AssertionError | Exception ex) {
            throw new RuntimeException(
                    String.format("Exception = %s, orgInfo = %s", ex.getMessage(), JsonUtils.serialize(orgInfo)), ex);
        }
    }
}
