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

import com.latticeengines.domain.exposed.cdl.CDLConstants;
import com.latticeengines.domain.exposed.playmaker.PlaymakerConstants;
import com.latticeengines.domain.exposed.playmakercore.Recommendation;
import com.latticeengines.domain.exposed.playmakercore.SynchronizationDestinationEnum;
import com.latticeengines.domain.exposed.pls.Play;
import com.latticeengines.domain.exposed.pls.PlayLaunch;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.playmakercore.entitymanager.RecommendationEntityMgr;
import com.latticeengines.playmakercore.service.LpiPMRecommendation;
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

    @Value("${playmaker.recommendations.years.keep:2}")
    private Double YEARS_TO_KEEP_RECOMMENDATIONS;

    private Tenant tenant;

    private Play play;

    private PlayLaunch playLaunch;

    private int maxUpdateRows = 20;
    private String syncDestination = "SFDC";
    private Map<String, String> orgInfo;

    @BeforeClass(groups = "deployment")
    public void setup() throws Exception {
        testPlayCreationHelper.setupTenantAndCreatePlay();

        tenant = testPlayCreationHelper.getTenant();
        play = testPlayCreationHelper.getPlay();
        playLaunch = testPlayCreationHelper.getPlayLaunch();

        List<Recommendation> recommendations = recommendationEntityMgr.findAll();
        Assert.assertTrue(CollectionUtils.isEmpty(recommendations));

        orgInfo = new HashMap<>();
        orgInfo.put(CDLConstants.ORG_ID, "DOID");
        orgInfo.put(CDLConstants.EXTERNAL_SYSTEM_TYPE, "CRM");

        createDummyRecommendations(maxUpdateRows * 2, new Date());
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
                SynchronizationDestinationEnum.SFDC, null, orgInfo);
        Assert.assertNotNull(recommendations2);
        Assert.assertEquals(count, recommendations2.size());
        AtomicLong idx = new AtomicLong(0);
        recommendations.stream() //
                .forEach(r -> {
                    Map<String, Object> r2 = recommendations2.get(idx.intValue());
                    idx.incrementAndGet();
                    Assert.assertNotNull(r2);
                    Assert.assertTrue(r2.size() > 0);

                    Assert.assertEquals(r2.get(PlaymakerConstants.ID), r.getId());
                    Assert.assertEquals(r2.get(PlaymakerConstants.PlayID + PlaymakerConstants.V2), r.getPlayId());
                    Assert.assertEquals(r2.get(PlaymakerConstants.LaunchID + PlaymakerConstants.V2), r.getLaunchId());
                    Assert.assertEquals(r2.get(PlaymakerConstants.LEAccountExternalID), r.getLeAccountExternalID());
                });
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
            recommendationEntityMgr.create(rec);
        }
    }
}
