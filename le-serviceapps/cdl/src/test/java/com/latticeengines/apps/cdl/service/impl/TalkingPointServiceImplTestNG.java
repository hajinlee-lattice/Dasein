package com.latticeengines.apps.cdl.service.impl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

import javax.inject.Inject;

import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.apps.cdl.entitymgr.PlayTypeEntityMgr;
import com.latticeengines.apps.cdl.service.DataCollectionService;
import com.latticeengines.apps.cdl.service.PlayService;
import com.latticeengines.apps.cdl.service.SegmentService;
import com.latticeengines.apps.cdl.service.TalkingPointService;
import com.latticeengines.apps.cdl.testframework.CDLFunctionalTestNGBase;
import com.latticeengines.common.exposed.util.NamingUtils;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.cdl.TalkingPointDTO;
import com.latticeengines.domain.exposed.cdl.TalkingPointPreview;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.domain.exposed.pls.Play;
import com.latticeengines.domain.exposed.pls.PlayType;

public class TalkingPointServiceImplTestNG extends CDLFunctionalTestNGBase {

    @Inject
    private TalkingPointService talkingPointService;

    @Inject
    private PlayTypeEntityMgr playTypeEntityMgr;

    @Inject
    private SegmentService segmentService;

    @Inject
    private DataCollectionService dataCollectionService;

    @Inject
    private PlayService playService;

    private static final String PLAY_DISPLAY_NAME = "Test TP Plays hard";
    private static final String PLAY_TYPE_DISPLAY_NAME = "TestTPPlayType";
    private static final String SEGMENT_NAME = "testTPSegment";
    private static final String CREATED_BY = "lattice@lattice-engines.com";
    private Play testPlay;
    private PlayType testPlayType;

    @BeforeClass(groups = "functional")
    public void setup() {
        setupTestEnvironment();
        testPlayType = createTestPlayType();
        testPlay = createTestPlay();
    }

    @Test(groups = "functional", dependsOnMethods = "testEmptyTalkingPointsSave")
    public void testTalkingPointsOperations() {
        List<TalkingPointDTO> tps = new ArrayList<>();

        TalkingPointDTO tp = new TalkingPointDTO();
        tp.setPlayName(testPlay.getName());
        tp.setOffset(1);
        tp.setTitle("Test TP Title");
        tp.setContent("PLS Deployment Test Talking Point no 1 {!Account.Website}");
        tps.add(tp);

        tps = talkingPointService.createOrUpdate(tps);
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
        tp2.setContent("PLS Deployment Test Talking Point no 2 {!Account.CompanyName}");
        tps.add(tp2);

        tps = talkingPointService.createOrUpdate(tps);
        Assert.assertNotNull(tps);
        Assert.assertEquals(tps.size(), 2);
        Assert.assertNotNull(tps.get(1).getPid());
        Assert.assertNotEquals(tps.get(1).getPid(), 0);
        Assert.assertNotNull(tps.get(1).getName());

        tp2 = tps.get(1);

        List<String> playDisplayNames = playService.findDependantPlayDisplayNames(Arrays.asList("Account.CompanyName", "Account" +
                ".Website"));
        Assert.assertEquals(playDisplayNames.size(), 1);
        Assert.assertEquals(playDisplayNames.get(0), testPlay.getDisplayName());

        // TODO: Turn On after fixing tenancy part of
        // talkingPointService.findDependantPlayIds
        // Set<String> playIds = talkingPointService
        // .findDependantPlayIds(Arrays.asList("Account.CompanyName",
        // "Account.Website"));
        // Assert.assertEquals(playIds.size(), 1);
        // Assert.assertEquals(new ArrayList<>(playIds).get(0), testPlay.getName());

        TalkingPointPreview preview = talkingPointService.getPreview(tp.getPlayName());
        Assert.assertNotNull(preview);
        Assert.assertNotNull(preview.getNotionObject());
        Assert.assertNotNull(preview.getNotionObject().getTalkingPoints());
        Assert.assertEquals(preview.getNotionObject().getTalkingPoints().size(), tps.size());
        Assert.assertEquals(preview.getNotionObject().getTalkingPoints().get(0).getBaseExternalID(), tp.getName());
        Assert.assertEquals(preview.getNotionObject().getTalkingPoints().get(1).getBaseExternalID(), tp2.getName());

        talkingPointService.publish(tp.getPlayName());
        List<TalkingPointDTO> dtps = talkingPointService.findAllByPlayName(tp.getPlayName(), true);
        Assert.assertNotNull(dtps);
        Assert.assertEquals(dtps.size(), tps.size());
        Assert.assertEquals(dtps.get(0).getName(), tp.getName());
        Assert.assertEquals(dtps.get(1).getName(), tp2.getName());
        Assert.assertEquals(dtps.get(0).getPlayName(), tp.getPlayName());

        talkingPointService.delete(tp.getName());
        talkingPointService.delete(tp2.getName());

        // TODO: Turn On after fixing tenancy part of
        // talkingPointService.findDependantPlayIds
        // playIds =
        // talkingPointService.findDependantPlayIds(Arrays.asList("Account.CompanyName",
        // "Account.Website"));
        // Assert.assertEquals(playIds.size(), 1);
        // Assert.assertEquals(new ArrayList<>(playIds).get(0), testPlay.getName());

        playDisplayNames = playService.findDependantPlayDisplayNames(Arrays.asList("Account.CompanyName", "Account" +
                ".Website"));
        Assert.assertEquals(playDisplayNames.size(), 1);
        Assert.assertEquals(playDisplayNames.get(0), testPlay.getDisplayName());

        tps = talkingPointService.findAllByPlayName(testPlay.getName(), false);
        Assert.assertNotNull(tps);
        Assert.assertEquals(tps.size(), 0);

        talkingPointService.revertToLastPublished(testPlay.getName());
        tps = talkingPointService.findAllByPlayName(testPlay.getName(), false);
        Assert.assertNotNull(tps);
        Assert.assertEquals(tps.size(), 2);
        Assert.assertEquals(tps.get(0).getName(), dtps.get(0).getName());
        Assert.assertEquals(tps.get(0).getPlayDisplayName(), testPlay.getDisplayName());
        Assert.assertEquals(tps.get(1).getName(), dtps.get(1).getName());
        Assert.assertEquals(tps.get(1).getPlayDisplayName(), testPlay.getDisplayName());

        talkingPointService.delete(tp.getName());
        talkingPointService.delete(tp2.getName());

        tps = talkingPointService.findAllByPlayName(tp.getPlayName(), false);
        Assert.assertNotNull(tps);
        Assert.assertEquals(tps.size(), 0);

        talkingPointService.publish(tp.getPlayName());
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

        tps = talkingPointService.createOrUpdate(tps);
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

        tps = talkingPointService.createOrUpdate(tps);
        Assert.assertNotNull(tps);
        Assert.assertEquals(tps.size(), 2);
        Assert.assertNotNull(tps.get(1).getPid());
        Assert.assertNotEquals(tps.get(1).getPid(), 0);
        Assert.assertNotNull(tps.get(1).getName());

        TalkingPointPreview preview = talkingPointService.getPreview(tp.getPlayName());
        Assert.assertNotNull(preview);
        Assert.assertNotNull(preview.getNotionObject());
        Assert.assertEquals(preview.getNotionObject().getPlayDisplayName(), testPlay.getDisplayName());
        Assert.assertNotNull(preview.getNotionObject().getTalkingPoints());
        Assert.assertEquals(preview.getNotionObject().getTalkingPoints().size(), tps.size());
        Assert.assertEquals(preview.getNotionObject().getTalkingPoints().get(0).getBaseExternalID(), testtp.getName());

        talkingPointService.publish(tp.getPlayName());
        List<TalkingPointDTO> dtps = talkingPointService.findAllByPlayName(tp.getPlayName(), true);
        Assert.assertNotNull(dtps);
        Assert.assertEquals(dtps.size(), tps.size());
        Assert.assertEquals(dtps.get(0).getName(), tp.getName());
        Assert.assertEquals(dtps.get(0).getPlayName(), tp.getPlayName());

        dtps = talkingPointService.findAllPublishedByTenant(mainCustomerSpace);
        Assert.assertNotNull(dtps);
        Assert.assertEquals(dtps.size(), tps.size());
        Assert.assertEquals(dtps.get(0).getPlayName(), testPlay.getName());
        Assert.assertEquals(dtps.get(0).getPlayDisplayName(), testPlay.getDisplayName());

        talkingPointService.delete(tp.getName());
        talkingPointService.delete(testtp.getName());
        talkingPointService.publish(tp.getPlayName());

        dtps = talkingPointService.findAllByPlayName(testPlay.getName(), true);
        Assert.assertEquals(dtps.size(), 0);
    }

    @AfterClass
    public void cleanup() {
        deletePlay(testPlay);
    }

    private void deletePlay(Play play) {
        playService.deleteByName(play.getName(), true);
    }

    private Play createTestPlay() {
        Play play = new Play();
        MetadataSegment targetSegment = createSegment(SEGMENT_NAME);
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

        return playService.createOrUpdate(play, mainTestTenant.getId());
    }

    private MetadataSegment createSegment(String segmentName) {
        MetadataSegment segment = new MetadataSegment();
        segment.setDisplayName(segmentName);
        segment.setName(NamingUtils.timestamp("testseg"));
        segment.setTenant(MultiTenantContext.getTenant());
        segment.setDataCollection(createDataCollection());
        return segmentService.createOrUpdateSegment(segment);
    }

    private DataCollection createDataCollection() {
        return dataCollectionService.createDefaultCollection();
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
        playTypeEntityMgr.create(playType);

        return playTypeEntityMgr.findById(playType.getId());
    }
}
