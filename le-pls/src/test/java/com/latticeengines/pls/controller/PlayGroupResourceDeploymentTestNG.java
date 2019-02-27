package com.latticeengines.pls.controller;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.pls.Play;
import com.latticeengines.domain.exposed.pls.PlayGroup;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.pls.functionalframework.PlsDeploymentTestNGBase;
import com.latticeengines.proxy.exposed.cdl.PlayProxy;
import com.latticeengines.testframework.exposed.domain.PlayLaunchConfig;
import com.latticeengines.testframework.service.impl.TestPlayCreationHelper;

@Component
public class PlayGroupResourceDeploymentTestNG extends PlsDeploymentTestNGBase {
    private static final Logger log = LoggerFactory.getLogger(PlayGroupResourceDeploymentTestNG.class);

    @Inject
    private PlayProxy playProxy;

    @Inject
    private TestPlayCreationHelper testPlayCreationHelper;

    private Play play;
    private Tenant tenant;
    private PlayGroup playGroup;
    private String playGroupName = "playGroupTest";


    @BeforeClass(groups = "deployment")
    public void setup() throws Exception {
        String existingTenant = null;// "LETest1546299140564";

        final PlayLaunchConfig playLaunchConfig =
                new PlayLaunchConfig.Builder().existingTenant(existingTenant).mockRatingTable(true).build();
        testPlayCreationHelper.setupTenantAndCreatePlay(playLaunchConfig);
        tenant = testPlayCreationHelper.getTenant();
    }

    @Test(groups = "deployment")
    public void testCreate() {
        // Test creating a new playGroup
        playGroup = new PlayGroup(tenant, playGroupName, "admin.le.com", "admin.le.com");
        playGroup.setId(null);
        playGroup = playProxy.createPlayGroup(tenant.getId(), playGroup);
        Assert.assertNotNull(playGroup.getId());
        Assert.assertNotEquals(playGroup.getPid(), 0);
        List<PlayGroup> Groups = playProxy.getPlayGroups(tenant.getId()).stream()
                .filter(pl -> pl.getDisplayName().equals(playGroupName)).collect(Collectors.toList());
        Assert.assertNotNull(Groups);
        Assert.assertEquals(Groups.size(), 1);
        playGroup = Groups.get(0);
    }

    @Test(groups = "deployment", dependsOnMethods = "testCreate")
    public void testGetById() {
        sleepToAllowDbWriterReaderSync();
        // Test getting the newly made playGroup by Id
        String playGroupId = playGroup.getId();
        PlayGroup getPlayGroup = playProxy.getPlayGroupById(tenant.getId(), playGroupId);
        Assert.assertNotNull(getPlayGroup);
        Assert.assertEquals(getPlayGroup.getDisplayName(), playGroup.getDisplayName());
    }


    @Test(groups = "deployment", dependsOnMethods = "testGetById")
    public void testAttachToPlay() {
        // Test attaching a play to playgroup
        play = testPlayCreationHelper.getPlay();
        Set<PlayGroup> set = new HashSet<PlayGroup>();
        set.add(playGroup);
        play.setPlayGroups(set);
        Play updatedPlay = playProxy.createOrUpdatePlay(tenant.getId(), play);

        // assert the playgroup in play equals playgroup created
        sleepToAllowDbWriterReaderSync();
        Assert.assertNotNull(updatedPlay);
        Assert.assertNotNull(updatedPlay.getPlayGroups());
        List<PlayGroup> playGroupListFromPlay = new ArrayList<PlayGroup>(updatedPlay.getPlayGroups());
        Assert.assertEquals(playGroupListFromPlay.get(0).getDisplayName(), playGroupName);

        // assert the play in playgroup equals play created
        sleepToAllowDbWriterReaderSync();
        String playGroupId = playGroup.getId();
        PlayGroup getPlayGroup = playProxy.getPlayGroupById(tenant.getId(), playGroupId);
        List<Play> playListFromPlayGroup = new ArrayList<Play>(getPlayGroup.getPlays());
        Assert.assertEquals(playListFromPlayGroup.get(0).getDisplayName(), play.getDisplayName());

    }

    @Test(groups = "deployment", dependsOnMethods = "testAttachToPlay")
    public void testUpdate() {
        // Test updating the newly made playGroup
        String updatedPlayGroupName = "playGroupTestPostUpdate";
        playGroup.setDisplayName(updatedPlayGroupName);
        playProxy.updatePlayGroup(tenant.getId(), playGroup.getId(), playGroup);
        sleepToAllowDbWriterReaderSync();
        PlayGroup updatedPlayGroup = playProxy.getPlayGroupById(tenant.getId(), playGroup.getId());
        Assert.assertNotNull(updatedPlayGroup);
        Assert.assertEquals(updatedPlayGroup.getDisplayName(), updatedPlayGroupName);
    }


    @Test(groups = "deployment", dependsOnMethods = "testUpdate")
    public void testDelete() {
        // Test deleting playGroup
        playProxy.deletePlayGroupById(tenant.getId(), playGroup.getId());
        sleepToAllowDbWriterReaderSync();
        List<PlayGroup> playGroupList = playProxy.getPlayGroups(tenant.getId());
        Assert.assertNotNull(playGroupList);
        List<PlayGroup> playGroups =
                playGroupList.stream().filter(pt -> pt.getId().equals(playGroup.getId())).collect(Collectors.toList());
        Assert.assertTrue(CollectionUtils.isEmpty(playGroups));

        // Test if also deleted on play side
        Play getPlay = playProxy.getPlay(tenant.getId(), play.getName());
        Assert.assertTrue(CollectionUtils.isEmpty(getPlay.getPlayGroups()));
    }

    @AfterClass(groups = {"deployment"})
    public void teardown() throws Exception {
        testPlayCreationHelper.cleanupArtifacts(true);
        log.info("Cleaned up all artifacts");
    }

    private void sleepToAllowDbWriterReaderSync() {
        try {
            Thread.sleep(10000);
        } catch (InterruptedException e) {
            // Ignore
        }
    }
}
