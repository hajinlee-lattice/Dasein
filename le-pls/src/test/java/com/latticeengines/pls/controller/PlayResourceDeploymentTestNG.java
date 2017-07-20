package com.latticeengines.pls.controller;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.springframework.beans.factory.annotation.Value;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.ResponseDocument;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.domain.exposed.pls.LaunchState;
import com.latticeengines.domain.exposed.pls.Play;
import com.latticeengines.domain.exposed.pls.PlayLaunch;
import com.latticeengines.domain.exposed.pls.TalkingPointDTO;
import com.latticeengines.pls.functionalframework.PlsDeploymentTestNGBase;
import com.latticeengines.proxy.exposed.pls.InternalResourceRestApiProxy;

public class PlayResourceDeploymentTestNG extends PlsDeploymentTestNGBase {

    private static final String PLAY_DISPLAY_NAME = "play hard";
    private static final String SEGMENT_NAME = "segment";
    private static final String CREATED_BY = "lattice@lattice-engines.com";
    private Play play;
    private String name;
    private PlayLaunch playLaunch;

    private long CURRENT_TIME_MILLIS = System.currentTimeMillis();
    private String LAUNCH_DESCRIPTION = "playLaunch done on " + CURRENT_TIME_MILLIS;

    @Value("${common.pls.url}")
    private String internalResourceHostPort;

    private InternalResourceRestApiProxy internalResourceRestApiProxy;

    @BeforeClass(groups = "deployment")
    public void setup() throws Exception {
        setupTestEnvironmentWithOneTenant();
        mainTestTenant = testBed.getMainTestTenant();
        switchToSuperAdmin();
        internalResourceRestApiProxy = new InternalResourceRestApiProxy(internalResourceHostPort);
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Test(groups = "deployment")
    public void getCrud() {
        Play createdPlay1 = restTemplate.postForObject(getRestAPIHostPort() + "/pls/play", createDefaultPlay(),
                Play.class);
        name = createdPlay1.getName();
        assertPlay(createdPlay1);

        List<TalkingPointDTO> tps = getTestTalkingPoints(name);
        ResponseDocument<?> createTPResponse = restTemplate.postForObject( //
                getRestAPIHostPort() + "/pls/dante/talkingpoints", //
                tps, //
                ResponseDocument.class);
        Assert.assertNotNull(createTPResponse);
        Assert.assertNull(createTPResponse.getErrors());

        Play createdPlay2 = restTemplate.postForObject(getRestAPIHostPort() + "/pls/play", createDefaultPlay(),
                Play.class);
        Assert.assertNotNull(createdPlay2);

        List<Play> playList = (List) restTemplate.getForObject(getRestAPIHostPort() + "/pls/play/", List.class);
        Assert.assertNotNull(playList);
        Assert.assertEquals(playList.size(), 2);

        Play retrievedPlay = restTemplate.getForObject(getRestAPIHostPort() + "/pls/play/" + name, Play.class);
        assertPlay(retrievedPlay);
        Assert.assertEquals(retrievedPlay.getTalkingPoints().size(), 2);

        String jsonValue = JsonUtils.serialize(retrievedPlay);
        Assert.assertNotNull(jsonValue);
        this.play = createdPlay1;
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

        internalResourceRestApiProxy.updatePlayLaunch(CustomerSpace.parse(mainTestTenant.getId()), name,
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
        System.out.println("retrievedPlayOverview is " + retrievedFullPlay);

        List<Play> retrievedFullPlayList = restTemplate.getForObject(getRestAPIHostPort() + "/pls/play", List.class);
        Assert.assertNotNull(retrievedFullPlayList);
        Assert.assertEquals(retrievedFullPlayList.size(), 2);
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Test(groups = "deployment", dependsOnMethods = { "testGetFullPlays" })
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
        Assert.assertNotNull(playLaunch.getLastUpdatedTimestamp());
        Assert.assertEquals(playLaunch.getLastUpdatedTimestamp(), playLaunch.getCreatedTimestamp());
        Assert.assertNotNull(playLaunch.getApplicationId());
        Assert.assertNotNull(playLaunch.getLaunchState());
        Assert.assertEquals(playLaunch.getLaunchState(), LaunchState.Launching);
    }

    private Play createDefaultPlay() {
        Play play = new Play();
        MetadataSegment segment = new MetadataSegment();
        segment.setDisplayName(SEGMENT_NAME);
        play.setDisplayName(PLAY_DISPLAY_NAME);
        play.setSegment(segment);
        play.setSegmentName(SEGMENT_NAME);
        play.setCreatedBy(CREATED_BY);
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
        Assert.assertEquals(play.getDisplayName(), PLAY_DISPLAY_NAME);
        Assert.assertEquals(play.getSegmentName(), SEGMENT_NAME);
    }
}
