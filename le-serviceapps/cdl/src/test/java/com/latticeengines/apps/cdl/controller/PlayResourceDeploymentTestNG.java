package com.latticeengines.apps.cdl.controller;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;

import javax.inject.Inject;

import org.springframework.beans.factory.annotation.Value;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.apps.cdl.testframework.CDLDeploymentTestNGBase;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.domain.exposed.multitenant.TalkingPointDTO;
import com.latticeengines.domain.exposed.pls.LaunchState;
import com.latticeengines.domain.exposed.pls.Play;
import com.latticeengines.domain.exposed.pls.PlayLaunch;
import com.latticeengines.domain.exposed.pls.RatingBucketName;
import com.latticeengines.domain.exposed.pls.RatingEngine;
import com.latticeengines.domain.exposed.pls.RatingEngineStatus;
import com.latticeengines.domain.exposed.pls.RatingEngineType;
import com.latticeengines.domain.exposed.pls.RatingModel;
import com.latticeengines.domain.exposed.pls.RatingRule;
import com.latticeengines.domain.exposed.pls.RuleBasedModel;
import com.latticeengines.domain.exposed.query.Restriction;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.metadata.service.SegmentService;
import com.latticeengines.proxy.exposed.cdl.PlayProxy;
import com.latticeengines.proxy.exposed.cdl.RatingEngineProxy;
import com.latticeengines.proxy.exposed.dante.TalkingPointProxy;

public class PlayResourceDeploymentTestNG extends CDLDeploymentTestNGBase {

    private static final String SEGMENT_NAME = "segment";
    private static final String CREATED_BY = "lattice@lattice-engines.com";
    private Play play;
    private String name;
    private PlayLaunch playLaunch;

    private Tenant tenant;

    @Value("${common.pls.url}")
    private String internalResourceHostPort;

    @Inject
    private SegmentService segmentService;

    private RatingEngine ratingEngine1;
    private MetadataSegment segment;

    @Inject
    private RatingEngineProxy ratingEngineProxy;

    @Inject
    private TalkingPointProxy talkingPointProxy;

    @Inject
    private PlayProxy playProxy;

    private boolean shouldSkipAutoTenantCreation = false;

    @BeforeClass(groups = "deployment")
    public void setup() throws Exception {
        if (shouldSkipAutoTenantCreation) {
            tenant = mainTestTenant;
        } else {
            setupTestEnvironment();
            tenant = testBed.getMainTestTenant();
        }

        MetadataSegment retrievedSegment = createSegment();

        createRatingEngine(retrievedSegment, new RatingRule());
    }

    public void setTenant(Tenant tenant) {
        this.tenant = tenant;
    }

    public RatingEngine createRatingEngine(MetadataSegment retrievedSegment, RatingRule ratingRule) {
        ratingEngine1 = new RatingEngine();
        ratingEngine1.setSegment(retrievedSegment);
        ratingEngine1.setCreatedBy(CREATED_BY);
        ratingEngine1.setType(RatingEngineType.RULE_BASED);
        ratingEngine1.setStatus(RatingEngineStatus.ACTIVE);

        RatingEngine createdRatingEngine = ratingEngineProxy.createOrUpdateRatingEngine(tenant.getId(), ratingEngine1);
        Assert.assertNotNull(createdRatingEngine);
        ratingEngine1.setId(createdRatingEngine.getId());

        List<RatingModel> models = ratingEngineProxy.getRatingModels(tenant.getId(), ratingEngine1.getId());
        for (RatingModel model : models) {
            if (model instanceof RuleBasedModel) {
                ((RuleBasedModel) model).setRatingRule(ratingRule);
                ratingEngineProxy.updateRatingModel(tenant.getId(), ratingEngine1.getId(), model.getId(), model);
            }
        }

        ratingEngine1 = ratingEngineProxy.getRatingEngine(tenant.getId(), ratingEngine1.getId());
        return ratingEngine1;
    }

    MetadataSegment createSegment() {
        return createSegment(null, null);
    }

    public MetadataSegment createSegment(Restriction accountRestriction, Restriction contactRestriction) {
        segment = new MetadataSegment();
        segment.setAccountRestriction(accountRestriction);
        segment.setContactRestriction(contactRestriction);
        segment.setDisplayName(SEGMENT_NAME);
        MetadataSegment createdSegment = segmentService
                .createOrUpdateSegment(CustomerSpace.parse(tenant.getId()).toString(), segment);
        MetadataSegment retrievedSegment = segmentService.findByName(CustomerSpace.parse(tenant.getId()).toString(),
                createdSegment.getName());
        Assert.assertNotNull(retrievedSegment);
        return retrievedSegment;
    }

    @Test(groups = "deployment")
    public void getCrud() {
        List<Play> playList = playProxy.getPlays(tenant.getId(), null, null);
        int existingPlays = playList == null ? 0 : playList.size();
        Play createdPlay1 = playProxy.createOrUpdatePlay(tenant.getId(), createDefaultPlay());
        name = createdPlay1.getName();
        play = createdPlay1;
        assertPlay(createdPlay1);

        List<TalkingPointDTO> tps = getTestTalkingPoints(name);
        List<TalkingPointDTO> createTPResponse = talkingPointProxy.createOrUpdate(tps,
                CustomerSpace.parse(tenant.getId()).toString());
        Assert.assertNotNull(createTPResponse);

        Play createdPlay2 = playProxy.createOrUpdatePlay(tenant.getId(), createDefaultPlay());
        Assert.assertNotNull(createdPlay2);

        playList = playProxy.getPlays(tenant.getId(), null, null);
        Assert.assertNotNull(playList);
        Assert.assertEquals(playList.size(), existingPlays + 2);

        playList = playProxy.getPlays(tenant.getId(), null, ratingEngine1.getId());
        Assert.assertNotNull(playList);
        Assert.assertEquals(playList.size(), 2);

        Play retrievedPlay = playProxy.getPlay(tenant.getId(), name);
        Assert.assertEquals(retrievedPlay.getTalkingPoints().size(), 2);

        String jsonValue = JsonUtils.serialize(retrievedPlay);
        Assert.assertNotNull(jsonValue);
        this.play = retrievedPlay;
    }

    @Test(groups = "deployment", dependsOnMethods = { "getCrud" })
    public void createPlayLaunch() {
        playLaunch = playProxy.createPlayLaunch(tenant.getId(), name, createDefaultPlayLaunch());
        assertPlayLaunch(playLaunch);
    }

    @Test(groups = "deployment", dependsOnMethods = { "createPlayLaunch" })
    private void searchPlayLaunch() {
        List<PlayLaunch> launchList = playProxy.getPlayLaunches(tenant.getId(), name,
                Arrays.asList(LaunchState.Failed));

        Assert.assertNotNull(launchList);
        Assert.assertEquals(launchList.size(), 0);

        playProxy.updatePlayLaunch(tenant.getId(), name, playLaunch.getLaunchId(), LaunchState.Launched);
        playProxy.updatePlayLaunchProgress(tenant.getId(), name, playLaunch.getLaunchId(), 100.0D, 10L, 8L, 25L, 0L,
                2L);

        launchList = playProxy.getPlayLaunches(tenant.getId(), name,
                Arrays.asList(LaunchState.Canceled, LaunchState.Failed, LaunchState.Launched));

        Assert.assertNotNull(launchList);
        Assert.assertEquals(launchList.size(), 1);

        launchList = playProxy.getPlayLaunches(tenant.getId(), name, Arrays.asList(LaunchState.Launched));

        Assert.assertNotNull(launchList);
        Assert.assertEquals(launchList.size(), 1);

        launchList = playProxy.getPlayLaunches(tenant.getId(), name, null);

        Assert.assertNotNull(launchList);
        Assert.assertEquals(launchList.size(), 1);

        launchList = playProxy.getPlayLaunches(tenant.getId(), name, Arrays.asList(LaunchState.Launching));

        Assert.assertNotNull(launchList);
        Assert.assertEquals(launchList.size(), 0);

        PlayLaunch retrievedLaunch = playProxy.getPlayLaunch(tenant.getId(), name, playLaunch.getLaunchId());

        Assert.assertNotNull(retrievedLaunch);
        Assert.assertEquals(retrievedLaunch.getLaunchState(), LaunchState.Launched);
        assertLaunchStats(retrievedLaunch.getAccountsSelected(), 10L);
        assertLaunchStats(retrievedLaunch.getAccountsLaunched(), 8L);
        assertLaunchStats(retrievedLaunch.getContactsLaunched(), 25L);
        assertLaunchStats(retrievedLaunch.getAccountsErrored(), 0L);
        assertLaunchStats(retrievedLaunch.getAccountsSuppressed(), 2L);
    }

    private void assertLaunchStats(Long count, long expectedVal) {
        Assert.assertNotNull(count);
        Assert.assertEquals(count.longValue(), expectedVal);
    }

    @Test(groups = "deployment", dependsOnMethods = { "searchPlayLaunch" })
    private void testGetFullPlays() {
        Play retrievedFullPlay = playProxy.getPlay(tenant.getId(), name);
        Assert.assertNotNull(retrievedFullPlay);
        Assert.assertNotNull(retrievedFullPlay.getLaunchHistory());
        Assert.assertNotNull(retrievedFullPlay.getLaunchHistory().getPlayLaunch());
        Assert.assertNotNull(retrievedFullPlay.getLaunchHistory().getMostRecentLaunch());
        // TODO will change to NotNull after integration with RatingEngine is
        // fully done
        // Assert.assertNotNull(retrievedFullPlay.getLaunchHistory().getNewAccountsNum());
        // Assert.assertNotNull(retrievedFullPlay.getLaunchHistory().getNewContactsNum());
        System.out.println("retrievedPlayOverview is " + retrievedFullPlay);

        List<Play> retrievedFullPlayList = playProxy.getPlays(tenant.getId(), null, null);
        Assert.assertNotNull(retrievedFullPlayList);
        Assert.assertEquals(retrievedFullPlayList.size(), 2);
    }

    @Test(groups = "deployment", dependsOnMethods = { "testGetFullPlays" })
    private void testIdempotentCreateOrUpdatePlays() {
        Play createdPlay1 = playProxy.createOrUpdatePlay(tenant.getId(), play);
        Assert.assertNotNull(createdPlay1.getTalkingPoints());

        List<Play> retrievedFullPlayList = playProxy.getPlays(tenant.getId(), null, null);
        Assert.assertNotNull(retrievedFullPlayList);
        Assert.assertEquals(retrievedFullPlayList.size(), 2);
    }

    @Test(groups = "deployment", dependsOnMethods = { "testIdempotentCreateOrUpdatePlays" })
    public void testDeletePlayLaunch() {
        deletePlayLaunch(name, playLaunch.getLaunchId());

        List<PlayLaunch> launchList = playProxy.getPlayLaunches(tenant.getId(), name,
                Arrays.asList(LaunchState.Launched));

        Assert.assertNotNull(launchList);
        Assert.assertEquals(launchList.size(), 0);

    }

    @Test(groups = "deployment", dependsOnMethods = { "testDeletePlayLaunch" })
    private void testPlayDelete() {
        List<Play> playList;
        Play retrievedPlay;
        deletePlay(name);
        retrievedPlay = playProxy.getPlay(tenant.getId(), name);
        Assert.assertNull(retrievedPlay);
        playList = playProxy.getPlays(tenant.getId(), null, null);
        Assert.assertNotNull(playList);
        Assert.assertEquals(playList.size(), 1);
    }

    public void deletePlay(String playName) {
        playProxy.deletePlay(tenant.getId(), playName);
    }

    public void deletePlayLaunch(String playName, String playLaunchId) {
        playProxy.deletePlayLaunch(tenant.getId(), playName, playLaunchId);
    }

    private void assertPlayLaunch(PlayLaunch playLaunch) {
        Assert.assertNotNull(playLaunch);
        Assert.assertNotNull(playLaunch.getLaunchId());
        Assert.assertNotNull(playLaunch.getPid());
        Assert.assertNotNull(playLaunch.getUpdated());
        Assert.assertNotNull(playLaunch.getCreated());
        Assert.assertNotNull(playLaunch.getApplicationId());
        Assert.assertNotNull(playLaunch.getLaunchState());
        assertBucketsToLaunch(playLaunch.getBucketsToLaunch());
        Assert.assertEquals(playLaunch.getLaunchState(), LaunchState.Launching);
    }

    private void assertBucketsToLaunch(Set<RatingBucketName> bucketsToLaunch) {
        Assert.assertNotNull(playLaunch.getBucketsToLaunch());
        Set<RatingBucketName> defaultBucketsToLaunch = new TreeSet<>(Arrays.asList(RatingBucketName.values()));
        Assert.assertEquals(bucketsToLaunch.size(), defaultBucketsToLaunch.size());
        for (RatingBucketName bucket : bucketsToLaunch) {
            Assert.assertTrue(defaultBucketsToLaunch.contains(bucket));
        }
    }

    private Play createDefaultPlay() {
        Play play = new Play();
        play.setCreatedBy(CREATED_BY);
        RatingEngine ratingEngine = new RatingEngine();
        ratingEngine.setId(ratingEngine1.getId());
        play.setRatingEngine(ratingEngine);
        return play;
    }

    private List<TalkingPointDTO> getTestTalkingPoints(String playName) {
        List<TalkingPointDTO> tps = new ArrayList<>();
        TalkingPointDTO tp = new TalkingPointDTO();
        tp.setName("plsTP1" + UUID.randomUUID());
        tp.setPlayName(playName);
        tp.setOffset(1);
        tp.setTitle("Test TP Title");
        tp.setContent("PLS Deployment Test Talking Point no 1");
        tps.add(tp);

        TalkingPointDTO tp1 = new TalkingPointDTO();

        tp1.setName("plsTP2" + UUID.randomUUID());
        tp1.setPlayName(playName);
        tp1.setOffset(2);
        tp1.setTitle("Test TP2 Title");
        tp1.setContent("PLS Deployment Test Talking Point no 2");
        tps.add(tp1);

        return tps;
    }

    private void assertPlay(Play play) {
        Assert.assertNotNull(play);
        Assert.assertEquals(play.getName(), name);
        Assert.assertNotNull(play.getRatingEngine());
        Assert.assertEquals(play.getRatingEngine().getId(), ratingEngine1.getId());
    }

    public Play getPlay() {
        return play;
    }

    public PlayLaunch getPlayLaunch() {
        return playLaunch;
    }

    private PlayLaunch createDefaultPlayLaunch() {
        PlayLaunch playLaunch = new PlayLaunch();
        playLaunch.setBucketsToLaunch(new HashSet<>(Arrays.asList(RatingBucketName.values())));
        return playLaunch;
    }

    public void setShouldSkipAutoTenantCreation(boolean shouldSkipAutoTenantCreation) {
        this.shouldSkipAutoTenantCreation = shouldSkipAutoTenantCreation;
    }

    public RatingEngine getRatingEngine() {
        return this.ratingEngine1;
    }
}
