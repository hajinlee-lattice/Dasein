package com.latticeengines.dante.service.impl;

import static com.latticeengines.domain.exposed.metadata.DataCollection.Version.Blue;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.TransactionCallbackWithoutResult;
import org.springframework.transaction.support.TransactionTemplate;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.NamingUtils;
import com.latticeengines.dante.service.TalkingPointService;
import com.latticeengines.dante.testframework.DanteTestNGBase;
import com.latticeengines.dante.testframework.testdao.TestDataCollectionDao;
import com.latticeengines.dante.testframework.testdao.TestMetadataSegmentDao;
import com.latticeengines.dante.testframework.testdao.TestPlayDao;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.dante.TalkingPointPreview;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.domain.exposed.multitenant.TalkingPointDTO;
import com.latticeengines.domain.exposed.pls.Play;
import com.latticeengines.domain.exposed.pls.PlayType;
import com.latticeengines.proxy.exposed.cdl.PlayProxy;

public class TalkingPointServiceImplTestNG extends DanteTestNGBase {

    @Autowired
    private TalkingPointService talkingPointService;

    @Autowired
    private TestPlayDao testPlayDao;
    
    @Autowired
    private TestMetadataSegmentDao testMetadataSegmentDao;

    @Autowired
    private TestDataCollectionDao testDataCollectionDao;
    
    private PlayProxy playProxy;

    private static final String PLAY_DISPLAY_NAME = "Test TP Plays hard";
    private static final String SEGMENT_NAME = "testTPSegment";
    private static final String CREATED_BY = "lattice@lattice-engines.com";
    private Play testPlay;
    private PlayType testPlayType;

    @BeforeClass(groups = "functional")
    public void setup() {
        super.setupRunEnvironment();
        testPlayType = createTestPlayType();
        testPlay = createTestPlay();
        playProxy = spy(new PlayProxy());
        ((TalkingPointServiceImpl) talkingPointService).setPlayProxy(playProxy);
        doReturn(testPlay).when(playProxy).getPlay(CustomerSpace.parse(mainTestTenant.getId()).toString(),
                testPlay.getName());
    }

    @Test(groups = "functional")
    public void testTalkingPointsOperations() {
        List<TalkingPointDTO> tps = new ArrayList<>();

        TalkingPointDTO tp = new TalkingPointDTO();
        tp.setPlayName(testPlay.getName());
        tp.setOffset(1);
        tp.setTitle("Test TP Title");
        tp.setContent("PLS Deployment Test Talking Point no 1");
        tps.add(tp);

        tps = talkingPointService.createOrUpdate(tps, mainTestTenant.getId());
        Assert.assertNotNull(tps);
        Assert.assertEquals(tps.size(), 1);
        Assert.assertNotNull(tps.get(0).getPid());
        Assert.assertNotEquals(tps.get(0).getPid(), 0);
        Assert.assertNotNull(tps.get(0).getName());

        tp = talkingPointService.findByName(tps.get(0).getName());
        Assert.assertNotNull(tp);

        tps = talkingPointService.findAllByPlayName(tp.getPlayName(), false);
        Assert.assertNotNull(tps);
        Assert.assertEquals(tps.size(), 1);
        Assert.assertEquals(tps.get(0).getPid(), tp.getPid());
        Assert.assertEquals(tps.get(0).getName(), tp.getName());

        TalkingPointDTO tp2 = new TalkingPointDTO();
        tp2.setPlayName(testPlay.getName());
        tp2.setOffset(2);
        tp2.setTitle("Test TP2 Title");
        tp2.setContent("PLS Deployment Test Talking Point no 2");
        tps.add(tp2);

        tps = talkingPointService.createOrUpdate(tps, mainTestTenant.getId());
        Assert.assertNotNull(tps);
        Assert.assertEquals(tps.size(), 2);
        Assert.assertNotNull(tps.get(1).getPid());
        Assert.assertNotEquals(tps.get(1).getPid(), 0);
        Assert.assertNotNull(tps.get(1).getName());

        tp2 = tps.get(1);

        TalkingPointPreview preview = talkingPointService.getPreview(tp.getPlayName(),
                mainTestCustomerSpace.toString());
        Assert.assertNotNull(preview);
        Assert.assertNotNull(preview.getNotionObject());
        Assert.assertNotNull(preview.getNotionObject().getTalkingPoints());
        Assert.assertEquals(preview.getNotionObject().getTalkingPoints().size(), tps.size());
        Assert.assertEquals(preview.getNotionObject().getTalkingPoints().get(0).getBaseExternalID(), tp.getName());
        Assert.assertEquals(preview.getNotionObject().getTalkingPoints().get(1).getBaseExternalID(), tp2.getName());

        talkingPointService.publish(tp.getPlayName(), mainTestCustomerSpace.toString());
        List<TalkingPointDTO> dtps = talkingPointService.findAllByPlayName(tp.getPlayName(), true);
        Assert.assertNotNull(dtps);
        Assert.assertEquals(dtps.size(), tps.size());
        Assert.assertEquals(dtps.get(0).getName(), tp.getName());
        Assert.assertEquals(dtps.get(1).getName(), tp2.getName());
        Assert.assertEquals(dtps.get(0).getPlayName(), tp.getPlayName());

        talkingPointService.delete(tp.getName());
        talkingPointService.delete(tp2.getName());

        tps = talkingPointService.findAllByPlayName(testPlay.getName(), false);
        Assert.assertNotNull(tps);
        Assert.assertEquals(tps.size(), 0);

        talkingPointService.revertToLastPublished(testPlay.getName(), mainTestCustomerSpace.toString());
        tps = talkingPointService.findAllByPlayName(testPlay.getName(), false);
        Assert.assertNotNull(tps);
        Assert.assertEquals(tps.size(), 2);
        Assert.assertEquals(tps.get(0).getName(), dtps.get(0).getName());
        Assert.assertEquals(tps.get(1).getName(), dtps.get(1).getName());

        talkingPointService.delete(tp.getName());
        talkingPointService.delete(tp2.getName());

        tps = talkingPointService.findAllByPlayName(tp.getPlayName(), false);
        Assert.assertNotNull(tps);
        Assert.assertEquals(tps.size(), 0);

        talkingPointService.publish(tp.getPlayName(), mainTestCustomerSpace.toString());
        dtps = talkingPointService.findAllByPlayName(tp.getPlayName(), true);
        Assert.assertNotNull(dtps);
        Assert.assertEquals(dtps.size(), 0);
    }

    @Test(groups = "functional")
    public void testEmptyTalkingPointsSave() {
        List<TalkingPointDTO> tps = new ArrayList<>();

        TalkingPointDTO tp = new TalkingPointDTO();
        tp.setPlayName(testPlay.getName());
        tp.setOffset(1);
        tp.setTitle("Test TP Title");
        tp.setContent("PLS Deployment Test Talking Point no 1");
        tps.add(tp);

        TalkingPointDTO testtp = new TalkingPointDTO();
        testtp.setPlayName(testPlay.getName());
        tps.add(testtp);

        tps = talkingPointService.createOrUpdate(tps, mainTestTenant.getId());
        Assert.assertNotNull(tps);
        Assert.assertEquals(tps.size(), 2);
        Assert.assertNotNull(tps.get(0).getPid());
        Assert.assertNotEquals(tps.get(0).getPid(), 0);
        Assert.assertNotNull(tps.get(0).getName());
        Assert.assertNotNull(tps.get(1).getName());
        Assert.assertNull(tps.get(1).getTitle());
        Assert.assertNull(tps.get(1).getContent());

        tp = talkingPointService.findByName(tps.get(0).getName());
        Assert.assertNotNull(tp);

        testtp = talkingPointService.findByName(tps.get(1).getName());

        tps = talkingPointService.findAllByPlayName(tp.getPlayName(), false);
        Assert.assertNotNull(tps);
        Assert.assertEquals(tps.size(), 2);
        Assert.assertEquals(tps.get(0).getPid(), tp.getPid());
        Assert.assertEquals(tps.get(0).getName(), tp.getName());
        Assert.assertNull(tps.get(1).getTitle());
        Assert.assertNull(tps.get(1).getContent());

        tps = talkingPointService.createOrUpdate(tps, mainTestTenant.getId());
        Assert.assertNotNull(tps);
        Assert.assertEquals(tps.size(), 2);
        Assert.assertNotNull(tps.get(1).getPid());
        Assert.assertNotEquals(tps.get(1).getPid(), 0);
        Assert.assertNotNull(tps.get(1).getName());

        TalkingPointPreview preview = talkingPointService.getPreview(tp.getPlayName(),
                mainTestCustomerSpace.toString());
        Assert.assertNotNull(preview);
        Assert.assertNotNull(preview.getNotionObject());
        Assert.assertNotNull(preview.getNotionObject().getTalkingPoints());
        Assert.assertEquals(preview.getNotionObject().getTalkingPoints().size(), tps.size());
        Assert.assertEquals(preview.getNotionObject().getTalkingPoints().get(0).getBaseExternalID(), testtp.getName());

        talkingPointService.publish(tp.getPlayName(), mainTestCustomerSpace.toString());
        List<TalkingPointDTO> dtps = talkingPointService.findAllByPlayName(tp.getPlayName(), true);
        Assert.assertNotNull(dtps);
        Assert.assertEquals(dtps.size(), tps.size());
        Assert.assertEquals(dtps.get(0).getName(), tp.getName());
        Assert.assertEquals(dtps.get(0).getPlayName(), tp.getPlayName());

        talkingPointService.delete(tp.getName());
        talkingPointService.delete(testtp.getName());
        talkingPointService.publish(tp.getPlayName(), mainTestCustomerSpace.toString());

        dtps = talkingPointService.findAllByPlayName(testPlay.getName(), true);
        Assert.assertEquals(dtps.size(), 0);
    }

    @AfterClass
    public void cleanup() {
        deletePlay(testPlay);
    }

    private void deletePlay(Play play) {
        PlatformTransactionManager ptm = applicationContext.getBean("transactionManager",
                PlatformTransactionManager.class);
        TransactionTemplate tx = new TransactionTemplate(ptm);
        tx.execute(new TransactionCallbackWithoutResult() {
            public void doInTransactionWithoutResult(TransactionStatus status) {
                testPlayDao.delete(play);
            }
        });
    }

    private Play createTestPlay() {
        Play play = new Play();
        MetadataSegment targetSegment = createMetadataSegment(SEGMENT_NAME);
        play.setDisplayName(PLAY_DISPLAY_NAME);
        play.setCreatedBy(CREATED_BY);
        play.setUpdatedBy(CREATED_BY);
        play.setTenant(mainTestTenant);
        play.setTenantId(mainTestTenant.getPid());
        play.setUpdated(new Date());
        play.setCreated(new Date());
        play.setName(play.generateNameStr());
        play.setPlayType(testPlayType);
        play.setTargetSegment(targetSegment);
        
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

    private MetadataSegment createMetadataSegment(String segmentName) {
        MetadataSegment segment = new MetadataSegment();
        segment.setDisplayName(segmentName);
        segment.setName(NamingUtils.timestamp("testseg"));
        segment.setTenant(MultiTenantContext.getTenant());
        segment.setDataCollection(createDataCollection());
        
        PlatformTransactionManager ptm = applicationContext.getBean("transactionManager",
                PlatformTransactionManager.class);
        TransactionTemplate tx = new TransactionTemplate(ptm);
        tx.execute(new TransactionCallbackWithoutResult() {
            public void doInTransactionWithoutResult(TransactionStatus status) {
                testMetadataSegmentDao.create(segment);
            }
        });
        
        return segment;
    }
    
    private DataCollection createDataCollection() {
        DataCollection dc = new DataCollection();
        dc.setName(NamingUtils.timestamp("DC"));
        dc.setVersion(Blue);
        
        dc.setTenant(MultiTenantContext.getTenant());
        PlatformTransactionManager ptm = applicationContext.getBean("transactionManager",
                PlatformTransactionManager.class);
        TransactionTemplate tx = new TransactionTemplate(ptm);
        tx.execute(new TransactionCallbackWithoutResult() {
            public void doInTransactionWithoutResult(TransactionStatus status) {
                testDataCollectionDao.create(dc);
            }
        });
        
        return dc;
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
}
