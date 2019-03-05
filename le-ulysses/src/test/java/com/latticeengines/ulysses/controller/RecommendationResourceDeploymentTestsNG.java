package com.latticeengines.ulysses.controller;

import java.util.Date;

import javax.inject.Inject;

import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.TransactionCallbackWithoutResult;
import org.springframework.transaction.support.TransactionTemplate;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.db.exposed.entitymgr.TenantEntityMgr;
import com.latticeengines.domain.exposed.playmakercore.Recommendation;
import com.latticeengines.domain.exposed.playmakercore.SynchronizationDestinationEnum;
import com.latticeengines.domain.exposed.pls.RatingBucketName;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.playmakercore.dao.RecommendationDao;
import com.latticeengines.playmakercore.entitymanager.RecommendationEntityMgr;
import com.latticeengines.ulysses.testframework.UlyssesDeploymentTestNGBase;

public class RecommendationResourceDeploymentTestsNG extends UlyssesDeploymentTestNGBase {

    @Inject
    private RecommendationEntityMgr recommendationEntityMgr;

    @Inject
    private RecommendationDao recommendationDao;

    @Inject
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
        recommendation.setPriorityID(RatingBucketName.A);

        recommendationEntityMgr.create(recommendation);

        try {
            Recommendation recc = getOAuth2RestTemplate()
                    .getForObject(getRecommendationResourceUrl() + recommendation.getId(), Recommendation.class);
            Assert.assertNotNull(recc);
            Assert.assertEquals(recc.getId(), recommendation.getId());
        } finally {
            deleteRecommendation(recommendation);
        }
    }

    private void deleteRecommendation(Recommendation recommendation) {
        PlatformTransactionManager ptm = applicationContext.getBean("dataTransactionManager",
                PlatformTransactionManager.class);
        TransactionTemplate tx = new TransactionTemplate(ptm);
        tx.execute(new TransactionCallbackWithoutResult() {
            public void doInTransactionWithoutResult(TransactionStatus status) {
                recommendationDao.delete(recommendation);
            }
        });
    }

}
