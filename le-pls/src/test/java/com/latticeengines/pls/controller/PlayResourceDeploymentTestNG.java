package com.latticeengines.pls.controller;

import java.util.List;

import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.domain.exposed.pls.Play;
import com.latticeengines.pls.functionalframework.PlsDeploymentTestNGBase;

public class PlayResourceDeploymentTestNG extends PlsDeploymentTestNGBase {

    private static final String PLAY_DISPLAY_NAME = "play hard";
    private static final String SEGMENT_NAME = "segment";
    private Play play;
    private String name;

    @BeforeClass(groups = "deployment")
    public void setup() throws Exception {
        setupTestEnvironmentWithOneTenant();
        play = createDefaultPlay();
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Test(groups = "deployment")
    public void getCrud() {
        Play createdPlay1 = restTemplate.postForObject(getRestAPIHostPort() + "/pls/play", play, Play.class);
        name = createdPlay1.getName();
        assertPlay(createdPlay1);
        Play createdPlay2 = restTemplate.postForObject(getRestAPIHostPort() + "/pls/play", play, Play.class);
        Assert.assertNotNull(createdPlay2);
        List<Play> playList = (List) restTemplate.getForObject(getRestAPIHostPort() + "/pls/play/", List.class);
        Assert.assertNotNull(playList);
        Assert.assertEquals(playList.size(), 2);
        Play retrievedPlay = restTemplate.getForObject(getRestAPIHostPort() + "/pls/play/" + name, Play.class);
        assertPlay(retrievedPlay);
        restTemplate.delete(getRestAPIHostPort() + "/pls/play/" + name);
        retrievedPlay = restTemplate.getForObject(getRestAPIHostPort() + "/pls/play/" + name, Play.class);
        Assert.assertNull(retrievedPlay);
        playList = (List) restTemplate.getForObject(getRestAPIHostPort() + "/pls/play/", List.class);
        Assert.assertNotNull(playList);
        Assert.assertEquals(playList.size(), 1);
    }

    private Play createDefaultPlay() {
        Play play = new Play();
        MetadataSegment segment = new MetadataSegment();
        segment.setName(SEGMENT_NAME);
        segment.setDisplayName(SEGMENT_NAME);
        play.setDisplayName(PLAY_DISPLAY_NAME);
        play.setSegment(segment);
        play.setSegmentName(SEGMENT_NAME);
        return play;
    }

    private void assertPlay(Play play) {
        Assert.assertNotNull(play);
        Assert.assertEquals(play.getName(), name);
        Assert.assertEquals(play.getDisplayName(), PLAY_DISPLAY_NAME);
        Assert.assertEquals(play.getSegmentName(), SEGMENT_NAME);
    }
}
