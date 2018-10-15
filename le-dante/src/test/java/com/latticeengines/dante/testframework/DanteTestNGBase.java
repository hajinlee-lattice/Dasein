package com.latticeengines.dante.testframework;

import java.util.Date;

import javax.inject.Inject;

import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.support.DirtiesContextTestExecutionListener;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.TransactionCallbackWithoutResult;
import org.springframework.transaction.support.TransactionTemplate;
import org.testng.Assert;
import org.testng.annotations.Listeners;

import com.latticeengines.apps.cdl.service.SegmentService;
import com.latticeengines.common.exposed.util.NamingUtils;
import com.latticeengines.dante.testframework.testdao.TestPlayDao;
import com.latticeengines.dante.testframework.testdao.TestPlayLaunchDao;
import com.latticeengines.dante.testframework.testdao.TestPlayTypeDao;
import com.latticeengines.dante.testframework.testdao.TestRatingEngineDao;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.domain.exposed.pls.LaunchState;
import com.latticeengines.domain.exposed.pls.Play;
import com.latticeengines.domain.exposed.pls.PlayLaunch;
import com.latticeengines.domain.exposed.pls.PlayType;
import com.latticeengines.domain.exposed.pls.RatingEngine;
import com.latticeengines.domain.exposed.pls.RatingEngineStatus;
import com.latticeengines.domain.exposed.pls.RatingEngineType;
import com.latticeengines.domain.exposed.query.frontend.FrontEndRestriction;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.testframework.exposed.service.CDLTestDataService;
import com.latticeengines.testframework.exposed.service.GlobalAuthTestBed;
import com.latticeengines.testframework.service.impl.GlobalAuthCleanupTestListener;
import com.latticeengines.testframework.service.impl.GlobalAuthFunctionalTestBed;

@Listeners({ GlobalAuthCleanupTestListener.class })
@TestExecutionListeners({ DirtiesContextTestExecutionListener.class })
@ContextConfiguration(locations = { "classpath:test-dante-context.xml" })
public class DanteTestNGBase extends AbstractTestNGSpringContextTests {

    @Inject
    protected GlobalAuthFunctionalTestBed functionalTestBed;

    @Inject
    private SegmentService segmentService;

    @Inject
    protected TestRatingEngineDao testRatingEngineDao;

    @Inject
    protected TestPlayDao testPlayDao;

    @Inject
    protected TestPlayTypeDao testPlayTypeDao;

    @Inject
    protected TestPlayLaunchDao testPlayLaunchDao;

    @Inject
    private CDLTestDataService cdlTestDataService;

    protected Tenant mainTestTenant;

    protected CustomerSpace mainTestCustomerSpace;

    protected MetadataSegment testMetadataSegment;
    protected RatingEngine testRatingEngine;
    protected Play testPlay;
    protected PlayType testPlayType;
    protected PlayLaunch testPlayLaunch;
    protected static final String PLAY_DISPLAY_NAME = "DeplTestTPPlay";
    protected static final String PLAY_TYPE_DISPLAY_NAME = "DeplTestTPPlayType";
    protected static final String SEGMENT_NAME = "segment";
    protected static final String CREATED_BY = "lattice@lattice-engines.com";

    protected void setupRunEnvironment() {
        getTestBed().bootstrap(1);
        mainTestTenant = getTestBed().getMainTestTenant();
        mainTestCustomerSpace = CustomerSpace.parse(mainTestTenant.getId());
        MultiTenantContext.setTenant(mainTestTenant);
    }

    // should only be used in deployment tests
    protected void createDependences() throws Exception {
        cdlTestDataService.populateData(mainTestTenant.getId(), 3);
        testMetadataSegment = createTestSegment();
        testRatingEngine = createTestRatingEngine();
        cdlTestDataService.mockRatingTableWithSingleEngine(mainTestTenant.getId(), testRatingEngine.getId(), null);
        testPlayType = createTestPlayType();
        testPlay = createTestPlay();
        testPlayLaunch = createTestPlayLaunch(testPlay);
    }

    private MetadataSegment createTestSegment() throws Exception {
        MetadataSegment segment = new MetadataSegment();
        segment.setAccountFrontEndRestriction(new FrontEndRestriction());
        segment.setDisplayName(SEGMENT_NAME);
        MetadataSegment createdSegment = segmentService.createOrUpdateSegment(segment);
        Thread.sleep(500);
        segmentService.updateSegmentCounts(createdSegment.getName());
        Assert.assertNotNull(createdSegment);
        return createdSegment;
    }

    private RatingEngine createTestRatingEngine() {
        RatingEngine ratingEngine = new RatingEngine();
        ratingEngine.setSegment(testMetadataSegment);
        ratingEngine.setCreatedBy(CREATED_BY);
        ratingEngine.setType(RatingEngineType.RULE_BASED);
        String id = RatingEngine.generateIdStr();
        ratingEngine.setId(id);
        ratingEngine.setTenant(mainTestTenant);
        ratingEngine.setStatus(RatingEngineStatus.INACTIVE);
        ratingEngine.setDisplayName(
                String.format(RatingEngine.DEFAULT_NAME_PATTERN, RatingEngine.DATE_FORMAT.format(new Date())));
        PlatformTransactionManager ptm = applicationContext.getBean("transactionManager",
                PlatformTransactionManager.class);
        TransactionTemplate tx = new TransactionTemplate(ptm);
        tx.execute(new TransactionCallbackWithoutResult() {
            public void doInTransactionWithoutResult(TransactionStatus status) {
                testRatingEngineDao.create(ratingEngine);
            }
        });

        return ratingEngine;
    }

    private Play createTestPlay() {
        Play play = new Play();
        MetadataSegment segment = new MetadataSegment();
        segment.setDisplayName(SEGMENT_NAME);
        play.setDisplayName(PLAY_DISPLAY_NAME);
        play.setTargetSegment(testRatingEngine.getSegment());
        play.setRatingEngine(testRatingEngine);
        play.setCreatedBy(CREATED_BY);
        play.setUpdatedBy(CREATED_BY);
        play.setTenant(mainTestTenant);
        play.setTenantId(mainTestTenant.getPid());
        play.setUpdated(new Date());
        play.setCreated(new Date());
        play.setName(play.generateNameStr());
        play.setPlayType(testPlayType);
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

    private PlayType createTestPlayType() {
        PlayType playType = new PlayType();
        playType.setDisplayName(PLAY_TYPE_DISPLAY_NAME);
        playType.setCreatedBy(CREATED_BY);
        playType.setUpdatedBy(CREATED_BY);
        playType.setTenant(mainTestTenant);
        playType.setTenantId(mainTestTenant.getPid());
        playType.setUpdated(new Date());
        playType.setCreated(new Date());
        playType.setId(PlayType.generateId());
        PlatformTransactionManager ptm = applicationContext.getBean("transactionManager",
                PlatformTransactionManager.class);
        TransactionTemplate tx = new TransactionTemplate(ptm);
        tx.execute(new TransactionCallbackWithoutResult() {
            public void doInTransactionWithoutResult(TransactionStatus status) {
                testPlayTypeDao.create(playType);
            }
        });

        return playType;
    }

    private PlayLaunch createTestPlayLaunch(Play play) {
        PlayLaunch playLaunch = new PlayLaunch();
        MetadataSegment segment = new MetadataSegment();
        segment.setDisplayName("TestSegment");
        playLaunch.setLaunchId(NamingUtils.uuid("WorkFlowTestPlayLaunch"));
        playLaunch.setPlay(play);
        playLaunch.setCreated(new Date());
        playLaunch.setCreatedBy(CREATED_BY);
        playLaunch.setTenantId(mainTestTenant.getPid());
        playLaunch.setTenant(mainTestTenant);
        playLaunch.setUpdated(new Date());
        playLaunch.setUpdatedBy(CREATED_BY);
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

    protected GlobalAuthTestBed getTestBed() {
        return functionalTestBed;
    }
}
