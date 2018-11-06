package com.latticeengines.playmaker.service.impl;

import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.support.DirtiesContextTestExecutionListener;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Listeners;
import org.testng.annotations.Test;

import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.cdl.CDLConstants;
import com.latticeengines.domain.exposed.playmaker.PlaymakerConstants;
import com.latticeengines.domain.exposed.playmakercore.Recommendation;
import com.latticeengines.domain.exposed.playmakercore.SynchronizationDestinationEnum;
import com.latticeengines.domain.exposed.pls.LaunchState;
import com.latticeengines.domain.exposed.pls.Play;
import com.latticeengines.domain.exposed.pls.PlayLaunch;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.playmaker.dao.impl.LpiPMRecommendationDaoAdapterImpl;
import com.latticeengines.playmaker.service.LpiPMAccountExtension;
import com.latticeengines.playmaker.service.LpiPMPlay;
import com.latticeengines.playmakercore.entitymanager.RecommendationEntityMgr;
import com.latticeengines.playmakercore.service.LpiPMRecommendation;
import com.latticeengines.proxy.exposed.cdl.PlayProxy;
import com.latticeengines.testframework.service.impl.GlobalAuthCleanupTestListener;
import com.latticeengines.testframework.service.impl.TestPlayCreationHelper;

@Listeners({ GlobalAuthCleanupTestListener.class })
@TestExecutionListeners({ DirtiesContextTestExecutionListener.class })
@ContextConfiguration(locations = { "classpath:test-testframework-cleanup-context.xml",
        "classpath:playmakercore-context.xml", "classpath:test-playmaker-context.xml" })
public class LpiPMRecommendationImplDeploymentTestNG extends AbstractTestNGSpringContextTests {

    @Inject
    private TestPlayCreationHelper testPlayCreationHelper;

    @Inject
    private RecommendationEntityMgr recommendationEntityMgr;

    @Inject
    private LpiPMRecommendation lpiPMRecommendation;

    @Inject
    private LpiPMAccountExtension lpiAccountExt;

    @Inject
    private LpiPMPlay lpiPMPlay;

    @Inject
    private PlayProxy playProxy;

    @Inject
    private LpiPMRecommendationDaoAdapterImpl lpiReDaoAdapter;

    @Value("${playmaker.recommendations.years.keep:2}")
    private Double YEARS_TO_KEEP_RECOMMENDATIONS;

    private Tenant tenant;

    private Play play;

    private PlayLaunch playLaunch;

    private Map<String, String> eloquaAppId1;
    private Map<String, String> eloquaAppId2;

    private Date launchTime;

    private int maxUpdateRows = 20;
    private String syncDestination = "SFDC";
    private Map<String, String> orgInfo;

    @BeforeClass(groups = "deployment")
    public void setup() throws Exception {
        eloquaAppId1 = new HashMap<String, String>();
        eloquaAppId2 = new HashMap<String, String>();
        eloquaAppId1.put(CDLConstants.AUTH_APP_ID, "lattice.eloqua01234");
        eloquaAppId2.put(CDLConstants.AUTH_APP_ID, "BIS01234");
        testPlayCreationHelper.setupTenantAndCreatePlay();

        tenant = testPlayCreationHelper.getTenant();

        play = testPlayCreationHelper.getPlay();
        playLaunch = testPlayCreationHelper.getPlayLaunch();
        orgInfo = testPlayCreationHelper.getOrgInfo();
        playProxy.updatePlayLaunch(MultiTenantContext.getCustomerSpace().toString(), play.getName(),
                playLaunch.getLaunchId(), LaunchState.Launched);
        // List<Recommendation> recommendations =
        // recommendationEntityMgr.findAll();
        // Assert.assertTrue(CollectionUtils.isEmpty(recommendations));
        // orgInfo = new HashMap<>();
        // orgInfo.put(CDLConstants.ORG_ID, "DOID");
        // orgInfo.put(CDLConstants.EXTERNAL_SYSTEM_TYPE, "CRM");
        launchTime = new Date();
        createDummyRecommendations(maxUpdateRows * 2, launchTime);
    }

    @Test(groups = "deployment")
    public void testRecommendations() throws Exception {

        List<Recommendation> recommendations = recommendationEntityMgr//
                .findRecommendations(new Date(0), 0, maxUpdateRows * 8, //
                        syncDestination, null, orgInfo);
        Assert.assertTrue(CollectionUtils.isNotEmpty(recommendations));
        int count = recommendations.size();
        Assert.assertEquals(count, maxUpdateRows * 2);

        List<Map<String, Object>> recommendations2 = lpiPMRecommendation.getRecommendations(0, 0, count, //
                SynchronizationDestinationEnum.SFDC, null, orgInfo, eloquaAppId2);
        Assert.assertNotNull(recommendations2);
        Assert.assertEquals(count, recommendations2.size());
        // AtomicLong idx = new AtomicLong(0);
        // recommendations.stream() //
        // .forEach(r -> {
        // Map<String, Object> r2 = recommendations2.get(idx.intValue());
        // idx.incrementAndGet();
        // Assert.assertNotNull(r2);
        // Assert.assertTrue(r2.size() > 0);
        // Assert.assertEquals(r2.get(PlaymakerConstants.ID), r.getId());
        // Assert.assertEquals(r2.get(PlaymakerConstants.PlayID +
        // PlaymakerConstants.V2), r.getPlayId());
        // Assert.assertEquals(r2.get(PlaymakerConstants.LaunchID +
        // PlaymakerConstants.V2), r.getLaunchId());
        // Assert.assertEquals(r2.get(PlaymakerConstants.LEAccountExternalID),
        // r.getLeAccountExternalID());
        // });
    }

    @Test(groups = "deployment", dependsOnMethods = { "testRecommendations" })
    public void testGetLastLaunchedRecommendation() {
        List<Map<String, Object>> result = lpiReDaoAdapter.getRecommendations(0, 0, 1000, 0, null, orgInfo,
                eloquaAppId1);
        System.out.println("\nThis is Recommendation List:");
        System.out.println(result.toString() + "\n");
        Assert.assertTrue(result.size() > 0);
    }

    @Test(groups = "deployment", dependsOnMethods = { "testRecommendations" })
    public void testGetAccountIdsByLaunchIds() {
        List<String> accounts = lpiAccountExt.getAccountIdsByRecommendationsInfo(false, 0L, 0L, 1000L, orgInfo);
        System.out.println("\nThis is AccountID List:");
        System.out.println(accounts.toString() + "\n");
        Assert.assertTrue(accounts.size() > 0);
    }

    @Test(groups = "deployment", dependsOnMethods = { "testGetLastLaunchedRecommendation" })
    public void testGetContacts() throws Exception {
        List<String> lunchIDs = lpiPMPlay.getLaunchIdsFromDashboard(false, 0, null, 0, orgInfo);
        System.out.println("This is launchIDs List:");
        System.out.println(lunchIDs.toString());

        List<Map<String, Object>> contacts = lpiReDaoAdapter.getContacts(0, 1, 1000, null, null, 140000000L, orgInfo,
                eloquaAppId1);
        Assert.assertNotNull(contacts);
        int count = contacts.size();
        Assert.assertTrue(count >= maxUpdateRows);
        System.out.println("This is Contacts List:");
        System.out.println(contacts.toString());
    }

    private void createDummyRecommendations(int newRecommendationsCount, Date launchDate) {
        while (newRecommendationsCount-- > 0) {
            Recommendation rec = new Recommendation();
            rec.setAccountId("Acc_" + launchDate.toInstant().toEpochMilli() + "_" + newRecommendationsCount);
            rec.setCompanyName("CN_" + launchDate.toInstant().toEpochMilli() + "_" + newRecommendationsCount);
            rec.setDeleted(false);
            rec.setDestinationOrgId(orgInfo.get(CDLConstants.ORG_ID));
            rec.setDestinationSysType(orgInfo.get(CDLConstants.EXTERNAL_SYSTEM_TYPE));
            rec.setId("ID_" + launchDate.toInstant().toEpochMilli() + "_" + newRecommendationsCount);
            rec.setLaunchDate(launchDate);
            rec.setLaunchId(playLaunch.getId());
            rec.setLeAccountExternalID("Acc_" + launchDate.toInstant().toEpochMilli() + "_" + newRecommendationsCount);
            rec.setPlayId(play.getName());
            rec.setRecommendationId("ID_" + launchDate.toInstant().toEpochMilli() + "_" + newRecommendationsCount);
            rec.setSynchronizationDestination(syncDestination);
            rec.setTenantId(tenant.getPid());
            String contactStr = "[{\"Email\":\"FirstName5763@com\",  \"Address\": \"null Dr\",  \"Phone\":\"248.813.2000\",\"State\":\"MI\",\"ZipCode\":\"48098-2815\","
                    + "\"ContactID\":\"" + String.valueOf(launchDate.toInstant().toEpochMilli()) + "\","
                    + "\"Country\":\"USA\",\"SfdcContactID\": \"\",\"City\": \"Troy\",\"ContactID\": \"5763\",\"Name\": \"FirstName5763 LastName5763\"}]";
            rec.setContacts(contactStr);
            recommendationEntityMgr.create(rec);
        }
    }
}
