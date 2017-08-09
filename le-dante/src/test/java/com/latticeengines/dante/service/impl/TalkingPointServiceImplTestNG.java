package com.latticeengines.dante.service.impl;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.TransactionCallbackWithoutResult;
import org.springframework.transaction.support.TransactionTemplate;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.dante.entitymgr.DanteTalkingPointEntityMgr;
import com.latticeengines.dante.service.TalkingPointService;
import com.latticeengines.dante.testFramework.DanteTestNGBase;
import com.latticeengines.dante.testFramework.testDao.TestPlayDao;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.dante.DanteTalkingPoint;
import com.latticeengines.domain.exposed.dante.TalkingPointPreview;
import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.domain.exposed.pls.Play;
import com.latticeengines.domain.exposed.pls.TalkingPointDTO;
import com.latticeengines.proxy.exposed.pls.InternalResourceRestApiProxy;

public class TalkingPointServiceImplTestNG extends DanteTestNGBase {

    @Autowired
    private TalkingPointService talkingPointService;

    @Autowired
    private DanteTalkingPointEntityMgr danteTalkingPointEntityMgr;

    @Autowired
    private TestPlayDao testPlayDao;

    private InternalResourceRestApiProxy spiedInternalResourceRestApiProxy;

    private static final String PLAY_DISPLAY_NAME = "Test TP Plays hard";
    private static final String SEGMENT_NAME = "testTPSegment";
    private static final String CREATED_BY = "lattice@lattice-engines.com";
    private Play testPlay;

    @BeforeClass(groups = "functional")
    public void setup() {
        testPlay = createTestPlay();
        spiedInternalResourceRestApiProxy = spy(new InternalResourceRestApiProxy("doesn't matter"));
        ((TalkingPointServiceImpl) talkingPointService)
                .setInternalResourceRestApiProxy(spiedInternalResourceRestApiProxy);
        doReturn(testPlay).when(spiedInternalResourceRestApiProxy)
                .findPlayByName(CustomerSpace.parse(mainTestTenant.getId()), testPlay.getName());
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

        tps = talkingPointService.findAllByPlayName(tp.getPlayName());
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
        List<DanteTalkingPoint> dtps = danteTalkingPointEntityMgr.findAllByPlayID(tp.getPlayName());
        Assert.assertNotNull(dtps);
        Assert.assertEquals(dtps.size(), tps.size());
        Assert.assertEquals(dtps.get(0).getCustomerID(), mainTestCustomerSpace.getTenantId());
        Assert.assertEquals(dtps.get(0).getExternalID(), tp.getName());
        Assert.assertEquals(dtps.get(1).getExternalID(), tp2.getName());
        Assert.assertEquals(dtps.get(0).getPlayExternalID(), tp.getPlayName());

        talkingPointService.delete(tp.getName());
        talkingPointService.delete(tp2.getName());

        tps = talkingPointService.findAllByPlayName(testPlay.getName());
        Assert.assertNotNull(tps);
        Assert.assertEquals(tps.size(), 0);

        talkingPointService.revertToLastPublished(testPlay.getName(), mainTestCustomerSpace.toString());
        tps = talkingPointService.findAllByPlayName(testPlay.getName());
        Assert.assertNotNull(tps);
        Assert.assertEquals(tps.size(), 2);
        Assert.assertEquals(tps.get(0).getName(), dtps.get(0).getExternalID());
        Assert.assertEquals(tps.get(1).getName(), dtps.get(1).getExternalID());

        talkingPointService.delete(tp.getName());
        talkingPointService.delete(tp2.getName());

        tps = talkingPointService.findAllByPlayName(tp.getPlayName());
        Assert.assertNotNull(tps);
        Assert.assertEquals(tps.size(), 0);

        talkingPointService.publish(tp.getPlayName(), mainTestCustomerSpace.toString());
        dtps = danteTalkingPointEntityMgr.findAllByPlayID(tp.getPlayName());
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

        tps = talkingPointService.findAllByPlayName(tp.getPlayName());
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
        Assert.assertEquals(preview.getNotionObject().getTalkingPoints().get(0).getBaseExternalID(), tp.getName());

        talkingPointService.publish(tp.getPlayName(), mainTestCustomerSpace.toString());
        List<DanteTalkingPoint> dtps = danteTalkingPointEntityMgr.findAllByPlayID(tp.getPlayName());
        Assert.assertNotNull(dtps);
        Assert.assertEquals(dtps.size(), tps.size());
        Assert.assertEquals(dtps.get(0).getCustomerID(), mainTestCustomerSpace.getTenantId());
        Assert.assertEquals(dtps.get(0).getExternalID(), tp.getName());
        Assert.assertEquals(dtps.get(0).getPlayExternalID(), tp.getPlayName());

        talkingPointService.delete(tp.getName());
        talkingPointService.delete(testtp.getName());
        talkingPointService.publish(tp.getPlayName(), mainTestCustomerSpace.toString());

        dtps = danteTalkingPointEntityMgr.findAllByPlayID(testPlay.getName());
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
        MetadataSegment segment = new MetadataSegment();
        segment.setDisplayName(SEGMENT_NAME);
        play.setDisplayName(PLAY_DISPLAY_NAME);
        play.setSegment(segment);
        play.setSegmentName(SEGMENT_NAME);
        play.setCreatedBy(CREATED_BY);
        play.setTenant(mainTestTenant);
        play.setTenantId(mainTestTenant.getPid());
        play.setLastUpdatedTimestamp(new Date());
        play.setTimestamp(new Date());

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
}
