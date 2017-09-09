package com.latticeengines.dante.testFramework;

import java.util.Date;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.support.DirtiesContextTestExecutionListener;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.TransactionCallbackWithoutResult;
import org.springframework.transaction.support.TransactionTemplate;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;

import com.latticeengines.dante.testFramework.testDao.TestPlayDao;
import com.latticeengines.dante.testFramework.testDao.TestPlayLaunchDao;
import com.latticeengines.dante.testFramework.testDao.TestRatingEngineDao;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.domain.exposed.pls.LaunchState;
import com.latticeengines.domain.exposed.pls.Play;
import com.latticeengines.domain.exposed.pls.PlayLaunch;
import com.latticeengines.domain.exposed.pls.RatingEngine;
import com.latticeengines.domain.exposed.pls.RatingEngineType;
import com.latticeengines.domain.exposed.query.frontend.FrontEndRestriction;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.metadata.service.SegmentService;
import com.latticeengines.testframework.service.impl.GlobalAuthFunctionalTestBed;

@TestExecutionListeners({ DirtiesContextTestExecutionListener.class })
@ContextConfiguration(locations = { "classpath:test-dante-context.xml" })
public class DanteTestNGBase extends AbstractTestNGSpringContextTests {
    @Autowired
    protected GlobalAuthFunctionalTestBed testBed;

    @Autowired
    private SegmentService segmentService;

    @Autowired
    protected TestRatingEngineDao testRatingEngineDao;

    @Autowired
    protected TestPlayDao testPlayDao;

    @Autowired
    protected TestPlayLaunchDao testPlayLaunchDao;

    protected Tenant mainTestTenant;

    protected CustomerSpace mainTestCustomerSpace;

    protected MetadataSegment testMetadataSegment;
    protected RatingEngine testRatingEngine;
    protected Play testPlay;
    protected PlayLaunch testPlayLaunch;
    protected static final String PLAY_DISPLAY_NAME = "DeplTestTPPlay";
    protected static final String SEGMENT_NAME = "segment";
    protected static final String CREATED_BY = "lattice@lattice-engines.com";

    @BeforeClass(groups = { "functional", "deployment" })
    public void setupRunEnvironment() throws Exception {
        testBed.bootstrap(1);
        mainTestTenant = testBed.getMainTestTenant();
        mainTestCustomerSpace = CustomerSpace.parse(mainTestTenant.getId());
    }

    public void createDependences() {
        testMetadataSegment = createTestSegment();
        testRatingEngine = createTestRatingEngine();
        testPlay = createTestPlay();
        testPlayLaunch = createTestPlayLaunch(testPlay);
    }

    private MetadataSegment createTestSegment() {
        MetadataSegment segment = new MetadataSegment();
        segment.setAccountFrontEndRestriction(new FrontEndRestriction());
        segment.setDisplayName(SEGMENT_NAME);
        MetadataSegment createdSegment = segmentService
                .createOrUpdateSegment(CustomerSpace.parse(mainTestTenant.getId()).toString(), segment);
        segment = segmentService.findByName(CustomerSpace.parse(mainTestTenant.getId()).toString(),
                createdSegment.getName());
        Assert.assertNotNull(segment);
        return segment;
    }

    private RatingEngine createTestRatingEngine() {
        RatingEngine ratingEngine = new RatingEngine();
        ratingEngine.setSegment(testMetadataSegment);
        ratingEngine.setCreatedBy(CREATED_BY);
        ratingEngine.setType(RatingEngineType.RULE_BASED);
        String id = RatingEngine.generateIdStr();
        ratingEngine.setId(id);
        ratingEngine.setTenant(mainTestTenant);
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
        play.setRatingEngine(testRatingEngine);
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

    protected void deleteTestMetadataSegment() {
        segmentService.deleteSegmentByName(CustomerSpace.parse(mainTestTenant.getId()).toString(),
                testMetadataSegment.getName());
    }
}
