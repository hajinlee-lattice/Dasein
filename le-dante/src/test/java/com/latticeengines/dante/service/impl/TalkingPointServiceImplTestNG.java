package com.latticeengines.dante.service.impl;

import com.latticeengines.dante.testFramework.DanteTestNGBase;

public class TalkingPointServiceImplTestNG extends DanteTestNGBase {

//    @Autowired
//    @InjectMocks
//    private TalkingPointService talkingPointService;
//
//    @Autowired
//    private TalkingPointEntityMgr danteTalkingPointEntityMgr;
//
//    @Autowired
//    private TestPlayDao testPlayDao;
//
//    @Autowired
//    protected SessionFactory sessionFactory;
//
//    @Mock
//    private InternalResourceRestApiProxy internalResourceRestApiProxy;
//
//    private final String externalID = "talkingPointFTestExtID";
//
//    private static final String PLAY_DISPLAY_NAME = "Test TP Plays hard";
//    private static final String SEGMENT_NAME = "testTPSegment";
//    private static final String CREATED_BY = "lattice@lattice-engines.com";
//    private Play testPlay;
//
//    @BeforeClass(groups = "functional")
//    public void setup() {
//        testPlay = createTestPlay();
//        initMocks();
//
//
//    }
//
//    private void initMocks() {
//        doReturn(testPlay)
//                .when(internalResourceRestApiProxy)
//                .findPlayByName(CustomerSpace.parse(mainTestTenant.getId()), testPlay.getName());
//    }
//
//    @Test(groups = "functional")
//    public void testCreateFromService() {
//        List<TalkingPointDTO> dtps = new ArrayList<>();
//
//        TalkingPointDTO tp = new TalkingPointDTO();
//        tp.setPlayName(testPlay.getName());
//        tp.setOffset(1);
//        tp.setTitle("Test TP Title");
//        tp.setContent("PLS Deployment Test Talking Point no 1");
//        dtps.add(tp);
//
//        talkingPointService.createOrUpdate(dtps, mainTestTenant.getId());
//
//        TalkingPointService spiedAnalyticPipelineService = spy((TalkingPointServiceImpl) talkingPointService);
//        // dtp = talkingPointService.findByExternalID(externalID);
//        // Assert.assertNotNull(dtp,
//        // "Failure Cause: Talking Point not found by extrenal ID where
//        // externalID =
//        // " + externalID);
//        // Assert.assertEquals(dtp.getValue(), "New Talking Point Test",
//        // "Failure
//        // Cause: Talking Point value incorrect");
//        // Assert.assertNotEquals(dtp.getLastModificationDate(),
//        // oldLastModificationDate,
//        // "Failure Cause: Lastmodification date not updated by
//        // createOrUpdate()");
//        //
//        // dtps = talkingPointService.findAllByPlayID("testFPlayExtID");
//        //
//        // Assert.assertEquals(dtps.size(), 1, "Failure Cause: Talking Points
//        // not
//        // found by findByPlayID");
//        //
//        // danteTalkingPointEntityMgr.delete(dtp);
//        //
//        // dtp = danteTalkingPointEntityMgr.findByField("External_ID",
//        // externalID);
//        // Assert.assertNull(dtp, "Failure Cause: Talking point was not
//        // deleted");
//
//        deletePlay(testPlay);
//    }
//
//    private void deletePlay(Play play) {
//        PlatformTransactionManager ptm = applicationContext.getBean("transactionManager",
//                PlatformTransactionManager.class);
//        TransactionTemplate tx = new TransactionTemplate(ptm);
//        tx.execute(new TransactionCallbackWithoutResult() {
//            public void doInTransactionWithoutResult(TransactionStatus status) {
//                testPlayDao.create(play);
//            }
//        });
//    }
//
//    private Play createTestPlay() {
//        Play play = new Play();
//        MetadataSegment segment = new MetadataSegment();
//        segment.setDisplayName(SEGMENT_NAME);
//        play.setDisplayName(PLAY_DISPLAY_NAME);
//        play.setSegment(segment);
//        play.setSegmentName(SEGMENT_NAME);
//        play.setCreatedBy(CREATED_BY);
//        play.setTenant(mainTestTenant);
//        play.setTenantId(mainTestTenant.getPid());
//        play.setLastUpdatedTimestamp(new Date());
//        play.setTimestamp(new Date());
//
//        PlatformTransactionManager ptm = applicationContext.getBean("transactionManager",
//                PlatformTransactionManager.class);
//        TransactionTemplate tx = new TransactionTemplate(ptm);
//        tx.execute(new TransactionCallbackWithoutResult() {
//            public void doInTransactionWithoutResult(TransactionStatus status) {
//                testPlayDao.create(play);
//            }
//        });
//        return play;
//    }

    // @Test(groups = "functional")
    // public void testPublish() {
    // talkingPointService.publish("play__01798207-a121-4e99-8ae3-eaba6b979c6e",
    // "LETest1499999180448.LETest1499999180448.Production");
    // }
}
