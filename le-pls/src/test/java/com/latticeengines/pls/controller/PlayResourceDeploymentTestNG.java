package com.latticeengines.pls.controller;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.dante.multitenant.TalkingPointDTO;
import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.domain.exposed.pls.LaunchState;
import com.latticeengines.domain.exposed.pls.Play;
import com.latticeengines.domain.exposed.pls.PlayLaunch;
import com.latticeengines.domain.exposed.pls.RatingEngine;
import com.latticeengines.domain.exposed.pls.RatingEngineType;
import com.latticeengines.domain.exposed.query.Restriction;
import com.latticeengines.domain.exposed.query.frontend.FrontEndRestriction;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.metadata.service.SegmentService;
import com.latticeengines.pls.entitymanager.RatingEngineEntityMgr;
import com.latticeengines.pls.functionalframework.PlsDeploymentTestNGBase;
import com.latticeengines.proxy.exposed.pls.InternalResourceRestApiProxy;

@Component
public class PlayResourceDeploymentTestNG extends PlsDeploymentTestNGBase {

    private static final String SEGMENT_NAME = "segment";
    private static final String CREATED_BY = "lattice@lattice-engines.com";
    private Play play;
    private String name;
    private PlayLaunch playLaunch;

    private Tenant tenant;

    @Value("${common.pls.url}")
    private String internalResourceHostPort;

    @Autowired
    private SegmentService segmentService;

    private RatingEngine ratingEngine1;
    private MetadataSegment segment;
    private InternalResourceRestApiProxy internalResourceRestApiProxy;

    @Autowired
    private RatingEngineEntityMgr ratingEngineEntityMgr;

    private boolean shouldSkipAutoTenantCreation = false;

    @BeforeClass(groups = "deployment")
    public void setup() throws Exception {
        if (shouldSkipAutoTenantCreation) {
            tenant = mainTestTenant;
        } else {
            setupTestEnvironmentWithOneTenant();
            tenant = testBed.getMainTestTenant();
        }
        switchToSuperAdmin();

        internalResourceRestApiProxy = new InternalResourceRestApiProxy(internalResourceHostPort);

        MetadataSegment retrievedSegment = createSegment();

        createRatingEngine(retrievedSegment);
    }

    public void createRatingEngine(MetadataSegment retrievedSegment) {
        ratingEngine1 = new RatingEngine();
        ratingEngine1.setSegment(retrievedSegment);
        ratingEngine1.setCreatedBy(CREATED_BY);
        ratingEngine1.setType(RatingEngineType.RULE_BASED);
        RatingEngine createdRatingEngine = ratingEngineEntityMgr.createOrUpdateRatingEngine(ratingEngine1,
                tenant.getId());
        Assert.assertNotNull(createdRatingEngine);
        ratingEngine1.setId(createdRatingEngine.getId());

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

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Test(groups = "deployment")
    public void getCrud() {
        List<Play> playList = (List) restTemplate.getForObject(getRestAPIHostPort() + "/pls/play/", List.class);
        int existingPlays = playList == null ? 0 : playList.size();
        Play createdPlay1 = restTemplate.postForObject(getRestAPIHostPort() + "/pls/play", createDefaultPlay(),
                Play.class);
        name = createdPlay1.getName();
        play = createdPlay1;
        assertPlay(createdPlay1);

        List<TalkingPointDTO> tps = getTestTalkingPoints(name);
        List<TalkingPointDTO> createTPResponse = restTemplate.postForObject( //
                getRestAPIHostPort() + "/pls/dante/talkingpoints", //
                tps, //
                List.class);
        Assert.assertNotNull(createTPResponse);

        Play createdPlay2 = restTemplate.postForObject(getRestAPIHostPort() + "/pls/play", createDefaultPlay(),
                Play.class);
        Assert.assertNotNull(createdPlay2);

        playList = (List) restTemplate.getForObject(getRestAPIHostPort() + "/pls/play/", List.class);
        Assert.assertNotNull(playList);
        Assert.assertEquals(playList.size(), existingPlays + 2);

        Play retrievedPlay = restTemplate.getForObject(getRestAPIHostPort() + "/pls/play/" + name, Play.class);
        assertPlay(retrievedPlay);
        Assert.assertEquals(retrievedPlay.getTalkingPoints().size(), 2);

        String jsonValue = JsonUtils.serialize(retrievedPlay);
        Assert.assertNotNull(jsonValue);
        this.play = retrievedPlay;
    }

    @Test(groups = "deployment", dependsOnMethods = { "getCrud" })
    public void createPlayLaunch() {
        playLaunch = restTemplate.postForObject(getRestAPIHostPort() + //
                "/pls/play/" + name + "/launches", null, PlayLaunch.class);

        assertPlayLaunch(playLaunch);
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Test(groups = "deployment", dependsOnMethods = { "createPlayLaunch" })
    private void searchPlayLaunch() {
        List<PlayLaunch> launchList = (List) restTemplate.getForObject(getRestAPIHostPort() + //
                "/pls/play/" + name + "/launches?launchStates=" + LaunchState.Failed, List.class);

        Assert.assertNotNull(launchList);
        Assert.assertEquals(launchList.size(), 0);

        internalResourceRestApiProxy.updatePlayLaunch(CustomerSpace.parse(tenant.getId()), name,
                playLaunch.getLaunchId(), LaunchState.Launched);

        launchList = (List) restTemplate.getForObject(getRestAPIHostPort() + //
                "/pls/play/" + name + "/launches?launchStates=" + LaunchState.Canceled + "&launchStates="
                + LaunchState.Failed + "&launchStates=" + LaunchState.Launched, List.class);

        Assert.assertNotNull(launchList);
        Assert.assertEquals(launchList.size(), 1);

        launchList = (List) restTemplate.getForObject(getRestAPIHostPort() + //
                "/pls/play/" + name + "/launches?launchStates=" + LaunchState.Launched, List.class);

        Assert.assertNotNull(launchList);
        Assert.assertEquals(launchList.size(), 1);

        launchList = (List) restTemplate.getForObject(getRestAPIHostPort() + //
                "/pls/play/" + name + "/launches", List.class);

        Assert.assertNotNull(launchList);
        Assert.assertEquals(launchList.size(), 1);

        launchList = (List) restTemplate.getForObject(getRestAPIHostPort() + //
                "/pls/play/" + name + "/launches?launchStates=" + LaunchState.Launching, List.class);

        Assert.assertNotNull(launchList);
        Assert.assertEquals(launchList.size(), 0);

        PlayLaunch retrievedLaunch = restTemplate.getForObject(getRestAPIHostPort() + //
                "/pls/play/" + name + "/launches/" + playLaunch.getLaunchId(), PlayLaunch.class);
        Assert.assertNotNull(retrievedLaunch);
        Assert.assertEquals(retrievedLaunch.getLaunchState(), LaunchState.Launched);
    }

    @SuppressWarnings("unchecked")
    @Test(groups = "deployment", dependsOnMethods = { "searchPlayLaunch" })
    private void testGetFullPlays() {
        Play retrievedFullPlay = restTemplate.getForObject(getRestAPIHostPort() + "/pls/play/" + name, Play.class);
        Assert.assertNotNull(retrievedFullPlay);
        Assert.assertNotNull(retrievedFullPlay.getLaunchHistory());
        Assert.assertNotNull(retrievedFullPlay.getLaunchHistory().getPlayLaunch());
        Assert.assertNotNull(retrievedFullPlay.getLaunchHistory().getMostRecentLaunch());
        // TODO will change to NotNull after integration with RatingEngine is
        // fully done
        // Assert.assertNotNull(retrievedFullPlay.getLaunchHistory().getNewAccountsNum());
        // Assert.assertNotNull(retrievedFullPlay.getLaunchHistory().getNewContactsNum());
        System.out.println("retrievedPlayOverview is " + retrievedFullPlay);

        List<Play> retrievedFullPlayList = restTemplate.getForObject(getRestAPIHostPort() + "/pls/play", List.class);
        Assert.assertNotNull(retrievedFullPlayList);
        Assert.assertEquals(retrievedFullPlayList.size(), 2);
    }

    @SuppressWarnings("unchecked")
    @Test(groups = "deployment", dependsOnMethods = { "testGetFullPlays" })
    private void testIdempotentCreateOrUpdatePlays() {
        Play createdPlay1 = restTemplate.postForObject(getRestAPIHostPort() + "/pls/play", play, Play.class);
        assertPlay(createdPlay1);
        Assert.assertNotNull(createdPlay1.getTalkingPoints());

        List<Play> retrievedFullPlayList = restTemplate.getForObject(getRestAPIHostPort() + "/pls/play", List.class);
        Assert.assertNotNull(retrievedFullPlayList);
        Assert.assertEquals(retrievedFullPlayList.size(), 2);
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Test(groups = "deployment", dependsOnMethods = { "testIdempotentCreateOrUpdatePlays" })
    private void deletePlayLaunch() {
        restTemplate.delete(getRestAPIHostPort() + "/pls/play/" + name + "/launches/" + playLaunch.getLaunchId());

        List<PlayLaunch> launchList = (List) restTemplate.getForObject(getRestAPIHostPort() + //
                "/pls/play/" + name + "/launches?launchStates=" + LaunchState.Launched, List.class);

        Assert.assertNotNull(launchList);
        Assert.assertEquals(launchList.size(), 0);

    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Test(groups = "deployment", dependsOnMethods = { "deletePlayLaunch" })
    private void testPlayDelete() {
        List<Play> playList;
        Play retrievedPlay;
        restTemplate.delete(getRestAPIHostPort() + "/pls/play/" + name);
        retrievedPlay = restTemplate.getForObject(getRestAPIHostPort() + "/pls/play/" + name, Play.class);
        Assert.assertNull(retrievedPlay);
        playList = (List) restTemplate.getForObject(getRestAPIHostPort() + "/pls/play/", List.class);
        Assert.assertNotNull(playList);
        Assert.assertEquals(playList.size(), 1);
    }

    private void assertPlayLaunch(PlayLaunch playLaunch) {
        Assert.assertNotNull(playLaunch);
        Assert.assertNotNull(playLaunch.getLaunchId());
        Assert.assertNotNull(playLaunch.getPid());
        Assert.assertNotNull(playLaunch.getUpdated());
        Assert.assertNotNull(playLaunch.getCreated());
        Assert.assertNotNull(playLaunch.getApplicationId());
        Assert.assertNotNull(playLaunch.getLaunchState());
        Assert.assertEquals(playLaunch.getLaunchState(), LaunchState.Launching);
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

    public void setShouldSkipAutoTenantCreation(boolean shouldSkipAutoTenantCreation) {
        this.shouldSkipAutoTenantCreation = shouldSkipAutoTenantCreation;
    }
}
