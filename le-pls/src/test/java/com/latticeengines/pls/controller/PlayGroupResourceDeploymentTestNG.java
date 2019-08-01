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
import com.latticeengines.testframework.exposed.domain.TestPlaySetupConfig;
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
    private PlayGroup playGroup1;
    private PlayGroup playGroup2;
    private String playGroupName1 = "playGroup1";
    private String playGroupName2 = "playGroup2";

    @Override
    @BeforeClass(groups = "deployment")
    public void setup() throws Exception {
        String existingTenant = null;// "LETest1546299140564";

        final TestPlaySetupConfig testPlaySetupConfig = new TestPlaySetupConfig.Builder().existingTenant(existingTenant)
                .mockRatingTable(true).build();
        testPlayCreationHelper.setupTenantAndCreatePlay(testPlaySetupConfig);
        tenant = testPlayCreationHelper.getTenant();
    }

    @Test(groups = "deployment")
    public void testCreate() {
        // Test creating a new playGroup
        playGroup1 = new PlayGroup(tenant, playGroupName1, "admin.le.com", "admin.le.com");
        playGroup1.setId(null);
        playGroup1 = playProxy.createPlayGroup(tenant.getId(), playGroup1);
        Assert.assertNotNull(playGroup1.getId());
        Assert.assertNotEquals(playGroup1.getPid(), 0);

        playGroup2 = new PlayGroup(tenant, playGroupName2, "admin.le.com", "admin.le.com");
        playGroup2.setId(null);
        playGroup2 = playProxy.createPlayGroup(tenant.getId(), playGroup2);
        Assert.assertNotNull(playGroup2.getId());
        Assert.assertNotEquals(playGroup2.getPid(), 0);
        List<PlayGroup> Groups = playProxy.getPlayGroups(tenant.getId());
        Assert.assertNotNull(Groups);
        Assert.assertEquals(Groups.size(), 2);
        playGroup1 = Groups.get(0);
        playGroup2 = Groups.get(1);
        Assert.assertEquals(playGroup1.getDisplayName(), playGroupName1);
        Assert.assertEquals(playGroup2.getDisplayName(), playGroupName2);
    }

    @Test(groups = "deployment", dependsOnMethods = "testCreate")
    public void testGetById() {
        sleepToAllowDbWriterReaderSync();
        // Test getting the newly made playGroup by Id
        String playGroupId = playGroup1.getId();
        PlayGroup getPlayGroup = playProxy.getPlayGroupById(tenant.getId(), playGroupId);
        Assert.assertNotNull(getPlayGroup);
        Assert.assertEquals(getPlayGroup.getDisplayName(), playGroup1.getDisplayName());
    }

    @Test(groups = "deployment", dependsOnMethods = "testGetById")
    public void testAttachToPlay() {
        // Test attaching a play to playgroup
        play = testPlayCreationHelper.getPlay();
        Set<PlayGroup> set = new HashSet<PlayGroup>();
        set.add(playGroup1);
        set.add(playGroup2);
        play.setPlayGroups(set);
        Play updatedPlay = playProxy.createOrUpdatePlay(tenant.getId(), play);

        // assert the playgroup in play equals playgroup created
        sleepToAllowDbWriterReaderSync();
        Assert.assertNotNull(updatedPlay);
        Assert.assertNotNull(updatedPlay.getPlayGroups());
        List<PlayGroup> playGroupListFromPlay = new ArrayList<PlayGroup>(updatedPlay.getPlayGroups());

        List<PlayGroup> playGroup1List = playGroupListFromPlay.stream()
                .filter(pl -> pl.getDisplayName().equals(playGroupName1)).collect(Collectors.toList());
        Assert.assertEquals(playGroup1List.size(), 1);
        List<PlayGroup> playGroup2List = playGroupListFromPlay.stream()
                .filter(pl -> pl.getDisplayName().equals(playGroupName1)).collect(Collectors.toList());
        Assert.assertEquals(playGroup2List.size(), 1);

        // test get all plays
        sleepToAllowDbWriterReaderSync();
        String playGroupId = playGroup1.getId();
        PlayGroup getPlayGroup = playProxy.getPlayGroupById(tenant.getId(), playGroupId);
        Assert.assertNotNull(getPlayGroup);
        List<Play> getAllPlays = playProxy.getPlays(tenant.getId(), false, null);
        Assert.assertNotNull(getAllPlays);
        play = getAllPlays.get(0);
        Assert.assertEquals(play.getPlayGroups().size(), 2);

    }

    @Test(groups = "deployment", dependsOnMethods = "testAttachToPlay")
    public void testUpdate() {
        // Test updating the newly made playGroup
        String updatedPlayGroupName = "playGroupTestPostUpdate";
        playGroup1.setDisplayName(updatedPlayGroupName);
        playProxy.updatePlayGroup(tenant.getId(), playGroup1.getId(), playGroup1);
        sleepToAllowDbWriterReaderSync();
        PlayGroup updatedPlayGroup = playProxy.getPlayGroupById(tenant.getId(), playGroup1.getId());
        Assert.assertNotNull(updatedPlayGroup);
        Assert.assertEquals(updatedPlayGroup.getDisplayName(), updatedPlayGroupName);
    }

    @Test(groups = "deployment", dependsOnMethods = "testUpdate")
    public void testDelete() {
        // Test deleting playGroup
        playProxy.deletePlayGroupById(tenant.getId(), playGroup1.getId());
        sleepToAllowDbWriterReaderSync();
        List<PlayGroup> playGroupList = playProxy.getPlayGroups(tenant.getId());
        Assert.assertNotNull(playGroupList);
        List<PlayGroup> playGroups = playGroupList.stream().filter(pt -> pt.getId().equals(playGroup1.getId()))
                .collect(Collectors.toList());
        Assert.assertTrue(CollectionUtils.isEmpty(playGroups));

        // Test if also deleted on play side
        Play getPlay = playProxy.getPlay(tenant.getId(), play.getName());
        Assert.assertEquals(getPlay.getPlayGroups().size(), 1);
    }

    @AfterClass(groups = { "deployment" })
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
