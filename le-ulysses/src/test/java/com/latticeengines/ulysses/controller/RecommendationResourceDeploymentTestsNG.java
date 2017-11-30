package com.latticeengines.ulysses.controller;

import java.util.Date;

import org.springframework.beans.factory.annotation.Autowired;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.playmakercore.Recommendation;
import com.latticeengines.domain.exposed.playmakercore.SynchronizationDestinationEnum;
import com.latticeengines.domain.exposed.pls.RuleBucketName;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.playmakercore.entitymanager.RecommendationEntityMgr;
import com.latticeengines.security.exposed.entitymanager.TenantEntityMgr;
import com.latticeengines.ulysses.testframework.UlyssesDeploymentTestNGBase;

public class RecommendationResourceDeploymentTestsNG extends UlyssesDeploymentTestNGBase {

    @Autowired
    private RecommendationEntityMgr recommendationEntityMgr;

    @Autowired
    private TenantEntityMgr tenantEntityMgr;

    private String getRecommendationResourceUrl() {
        return ulyssesHostPort + "/ulysses/recommendations/";
    }

    static {
        CLIENT_ID = UlyssesSupportedClients.CLIENT_ID_PM;
    }

    @Test(groups = "deployment")
    public void testGetRecommendation() {
        Tenant testTenant = deploymentTestBed.getMainTestTenant();
        testTenant = tenantEntityMgr.findByTenantId(testTenant.getId());
        Recommendation recommendation = new Recommendation();
        recommendation.setDescription("Some Description");
        recommendation.setLaunchId("LAUNCH_ID");
        recommendation.setLaunchDate(new Date());
        recommendation.setPlayId("PLAY_ID");
        recommendation.setAccountId("ACCOUNT_ID");
        recommendation.setLeAccountExternalID("ACCOUNT_ID");
        recommendation.setTenantId(testTenant.getPid());
        recommendation.setContacts(null);
        recommendation.setSynchronizationDestination(SynchronizationDestinationEnum.SFDC.toString());
        recommendation.setPriorityID(RuleBucketName.A);

        recommendationEntityMgr.create(recommendation);

        try {
            Recommendation recc = getOAuth2RestTemplate()
                    .getForObject(getRecommendationResourceUrl() + recommendation.getId(), Recommendation.class);
            Assert.assertNotNull(recc);
            Assert.assertEquals(recc.getId(), recommendation.getAccountId());
        } finally {
            recommendationEntityMgr.delete(recommendation);
        }
    }

}
