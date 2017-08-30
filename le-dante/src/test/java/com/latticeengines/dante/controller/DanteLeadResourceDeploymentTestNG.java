package com.latticeengines.dante.controller;

import java.util.Date;
import java.util.UUID;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.support.DirtiesContextTestExecutionListener;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.TransactionCallbackWithoutResult;
import org.springframework.transaction.support.TransactionTemplate;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.dante.entitymgr.DanteLeadEntityMgr;
import com.latticeengines.dante.testFramework.DanteTestNGBase;
import com.latticeengines.dante.testFramework.testDao.TestPlayDao;
import com.latticeengines.dante.testFramework.testDao.TestPlayLaunchDao;
import com.latticeengines.domain.exposed.dante.DanteLead;
import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.domain.exposed.playmakercore.Recommendation;
import com.latticeengines.domain.exposed.pls.LaunchState;
import com.latticeengines.domain.exposed.pls.Play;
import com.latticeengines.domain.exposed.pls.PlayLaunch;
import com.latticeengines.domain.exposed.pls.RuleBucketName;
import com.latticeengines.proxy.exposed.dante.DanteLeadProxy;

@TestExecutionListeners({ DirtiesContextTestExecutionListener.class })
@ContextConfiguration(locations = { "classpath:test-dante-context.xml" })
public class DanteLeadResourceDeploymentTestNG extends DanteTestNGBase {
    @Autowired
    private DanteLeadProxy danteLeadProxy;

    @Autowired
    private DanteLeadEntityMgr danteLeadEntityMgr;

    @Autowired
    private TestPlayDao testPlayDao;

    @Autowired
    private TestPlayLaunchDao testPlayLaunchDao;

    @Value("${common.pls.url}")
    private String internalResourceHostPort;

    private Play testPlay;
    private PlayLaunch testPlayLaunch;
    private static final String PLAY_DISPLAY_NAME = "DeplTestTPPlay";
    private static final String SEGMENT_NAME = "segment";
    private static final String CREATED_BY = "lattice@lattice-engines.com";

    @BeforeClass(groups = "deployment")
    public void setup() {
        testPlay = createTestPlay();
        testPlayLaunch = createTestPlayLaunch(testPlay);
    }

    @Test(groups = "deployment")
    public void TestCreateRecommendation() {
        Recommendation testRec = getTestRecommendation();
        danteLeadProxy.create(testRec, mainTestCustomerSpace.toString());

        DanteLead fromDante = danteLeadEntityMgr.findByField("externalID", testRec.getId());
        Assert.assertNotNull(fromDante);

        danteLeadEntityMgr.delete(fromDante);
    }

    private Recommendation getTestRecommendation() {
        Recommendation rec = new Recommendation();
        rec.setAccountId("SomeAccountID");
        rec.setCompanyName("Some Company Name");
        rec.setRecommendationId(UUID.randomUUID().toString());
        rec.setDescription(testPlay.getDescription());
        rec.setLastUpdatedTimestamp(new Date());
        rec.setLaunchDate(new Date());
        rec.setLaunchId(testPlayLaunch.getLaunchId());
        rec.setLeAccountExternalID("SomeAccountID");
        rec.setLikelihood(null);
        rec.setMonetaryValue(null);
        rec.setPlayId(testPlay.getName());
        rec.setPriorityID(RuleBucketName.A);
        rec.setPriorityDisplayName("A");
        rec.setRecommendationId("10");
        rec.setSfdcAccountID("SomeAccountID");
        rec.setSfdcAccountID("SomeSFDCAccountID");
        rec.setTenantId(123L);
        return rec;
    }

    @AfterClass(groups = "deployment")
    public void cleanup() {
        deleteTestPlayLaunch();
        deletePlay();
    }

    private Play createTestPlay() {
        Play play = new Play();
        MetadataSegment segment = new MetadataSegment();
        segment.setDisplayName(SEGMENT_NAME);
        play.setDisplayName(PLAY_DISPLAY_NAME);
        play.setSegment(segment);
        play.setSegmentName(SEGMENT_NAME);
        play.setCreatedBy(CREATED_BY);
        play.setTenant(mainTestTenant);
        play.setTenantId(mainTestTenant.getPid());
        play.setUpdated(new Date());
        play.setCreated(new Date());
        play.setName(play.generateNameStr());
        PlatformTransactionManager ptm = applicationContext.getBean("transactionManager",
                PlatformTransactionManager.class);
        TransactionTemplate tx = new TransactionTemplate(ptm);
        tx.execute(new TransactionCallbackWithoutResult() {
            public void doInTransactionWithoutResult(TransactionStatus status) {
                testPlayDao.create(play);
            }
        });

        return play;
    }

    private PlayLaunch createTestPlayLaunch(Play play) {
        PlayLaunch playLaunch = new PlayLaunch();
        MetadataSegment segment = new MetadataSegment();
        segment.setDisplayName("TestSegment");
        playLaunch.setLaunchId("WorkFlowTestPlayLaunch");
        playLaunch.setPlay(play);
        playLaunch.setCreated(new Date());
        playLaunch.setTenantId(mainTestTenant.getPid());
        playLaunch.setTenant(mainTestTenant);
        playLaunch.setUpdated(new Date());
        playLaunch.setLaunchState(LaunchState.Launching);

        PlatformTransactionManager ptm = applicationContext.getBean("transactionManager",
                PlatformTransactionManager.class);
        TransactionTemplate tx = new TransactionTemplate(ptm);
        tx.execute(new TransactionCallbackWithoutResult() {
            public void doInTransactionWithoutResult(TransactionStatus status) {
                testPlayLaunchDao.create(playLaunch);
            }
        });
        return playLaunch;
    }

    private void deletePlay() {
        PlatformTransactionManager ptm = applicationContext.getBean("transactionManager",
                PlatformTransactionManager.class);
        TransactionTemplate tx = new TransactionTemplate(ptm);
        tx.execute(new TransactionCallbackWithoutResult() {
            public void doInTransactionWithoutResult(TransactionStatus status) {
                testPlayDao.delete(testPlay);
            }
        });
    }

    private void deleteTestPlayLaunch() {
        PlatformTransactionManager ptm = applicationContext.getBean("transactionManager",
                PlatformTransactionManager.class);
        TransactionTemplate tx = new TransactionTemplate(ptm);
        tx.execute(new TransactionCallbackWithoutResult() {
            public void doInTransactionWithoutResult(TransactionStatus status) {
                testPlayLaunchDao.delete(testPlayLaunch);
            }
        });
    }
}
