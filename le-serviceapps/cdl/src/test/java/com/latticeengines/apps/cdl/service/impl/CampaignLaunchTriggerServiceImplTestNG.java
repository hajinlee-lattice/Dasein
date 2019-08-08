package com.latticeengines.apps.cdl.service.impl;

import java.util.Date;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.apps.cdl.entitymgr.LookupIdMappingEntityMgr;
import com.latticeengines.apps.cdl.entitymgr.PlayEntityMgr;
import com.latticeengines.apps.cdl.entitymgr.PlayLaunchChannelEntityMgr;
import com.latticeengines.apps.cdl.entitymgr.PlayLaunchEntityMgr;
import com.latticeengines.apps.cdl.service.CampaignLaunchTriggerService;
import com.latticeengines.apps.cdl.service.PlayLaunchChannelService;
import com.latticeengines.apps.cdl.service.PlayLaunchService;
import com.latticeengines.apps.cdl.service.PlayTypeService;
import com.latticeengines.apps.cdl.testframework.CDLFunctionalTestNGBase;
import com.latticeengines.common.exposed.util.NamingUtils;
import com.latticeengines.domain.exposed.cdl.CDLExternalSystemName;
import com.latticeengines.domain.exposed.cdl.CDLExternalSystemType;
import com.latticeengines.domain.exposed.cdl.LaunchType;
import com.latticeengines.domain.exposed.pls.LaunchState;
import com.latticeengines.domain.exposed.pls.LookupIdMap;
import com.latticeengines.domain.exposed.pls.Play;
import com.latticeengines.domain.exposed.pls.PlayLaunch;
import com.latticeengines.domain.exposed.pls.PlayLaunchChannel;
import com.latticeengines.domain.exposed.pls.PlayType;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.security.exposed.service.TenantService;

public class CampaignLaunchTriggerServiceImplTestNG extends CDLFunctionalTestNGBase {

    @Autowired
    private PlayLaunchChannelEntityMgr playLaunchChannelEntityMgr;

    @Autowired
    private PlayLaunchChannelService playLaunchChannelService;

    @Autowired
    private CampaignLaunchTriggerService campaignLaunchTriggerService;

    @Autowired
    private PlayEntityMgr playEntityMgr;

    @Autowired
    private LookupIdMappingEntityMgr lookupIdMappingEntityMgr;

    @Autowired
    private PlayLaunchEntityMgr playLaunchEntityMgr;

    @Autowired
    private PlayLaunchService playLaunchService;

    @Autowired
    private PlayTypeService playTypeService;

    @Autowired
    private TenantService tenantService;

    private Play play;

    private LookupIdMap lookupIdMap1;
    private LookupIdMap lookupIdMap2;
    private PlayLaunchChannel playLaunchChannel1;
    private PlayLaunchChannel playLaunchChannel2;
    private PlayLaunch playLaunch1;
    private PlayLaunch playLaunch2;

    List<PlayType> types;

    private String orgId1 = "org1";
    private String orgName1 = "salesforce_org";

    private String orgId2 = "org2";
    private String orgName2 = "marketo_org";

    private long CURRENT_TIME_MILLIS = System.currentTimeMillis();

    private String NAME = "play" + CURRENT_TIME_MILLIS;
    private String DISPLAY_NAME = "play Harder";
    private String CREATED_BY = "lattice@lattice-engines.com";

    @BeforeClass(groups = "functional")
    public void setup() throws Exception {
        setupTestEnvironmentWithDummySegment();

        types = playTypeService.getAllPlayTypes(mainCustomerSpace);
        play = new Play();
        play.setName(NAME);
        play.setTenant(mainTestTenant);
        play.setDisplayName(DISPLAY_NAME);
        play.setPlayType(types.get(0));
        Date timestamp = new Date(System.currentTimeMillis());
        play.setCreated(timestamp);
        play.setUpdated(timestamp);
        play.setCreatedBy(CREATED_BY);
        play.setUpdatedBy(CREATED_BY);
        play.setTargetSegment(testSegment);

        playEntityMgr.create(play);
        Assert.assertNotNull(play);

        lookupIdMap1 = new LookupIdMap();
        lookupIdMap1.setExternalSystemType(CDLExternalSystemType.CRM);
        lookupIdMap1.setExternalSystemName(CDLExternalSystemName.Salesforce);
        lookupIdMap1.setOrgId(orgId1);
        lookupIdMap1.setOrgName(orgName1);
        lookupIdMap1 = lookupIdMappingEntityMgr.createExternalSystem(lookupIdMap1);
        Assert.assertNotNull(lookupIdMap1);
        lookupIdMap2 = new LookupIdMap();
        lookupIdMap2.setExternalSystemType(CDLExternalSystemType.MAP);
        lookupIdMap2.setExternalSystemName(CDLExternalSystemName.Marketo);
        lookupIdMap2.setOrgId(orgId2);
        lookupIdMap2.setOrgName(orgName2);
        lookupIdMap2 = lookupIdMappingEntityMgr.createExternalSystem(lookupIdMap2);
        Assert.assertNotNull(lookupIdMap2);

        playLaunchChannel1 = createPlayLaunchChannel(play, lookupIdMap1);
        playLaunchChannel2 = createPlayLaunchChannel(play, lookupIdMap2);
        playLaunchChannel1.setIsAlwaysOn(true);
        playLaunchChannel2.setIsAlwaysOn(true);
        playLaunchChannelEntityMgr.create(playLaunchChannel1);
        Assert.assertNotNull(playLaunchChannel1);
        playLaunchChannelEntityMgr.create(playLaunchChannel2);
        Assert.assertNotNull(playLaunchChannel2);
        playLaunch1 = playLaunchChannelService.createPlayLaunchFromChannel(playLaunchChannel1, play);
        Assert.assertNotNull(playLaunch1);
        playLaunch2 = playLaunchChannelService.createPlayLaunchFromChannel(playLaunchChannel2, play);
        Assert.assertNotNull(playLaunch2);

    }

    @Test(groups = "functional")
    public void testTriggerQueuedLaunches() throws InterruptedException {
        List<PlayLaunch> queuedPlayLaunches = playLaunchService.getByStateAcrossTenants(LaunchState.Queued, null);
        Assert.assertEquals(queuedPlayLaunches.size(), 2);
        List<PlayLaunch> launchingPlayLaunches = playLaunchService.getByStateAcrossTenants(LaunchState.Launching, null);
        Assert.assertEquals(launchingPlayLaunches.size(), 0);

        campaignLaunchTriggerService.triggerQueuedLaunches();
        Thread.sleep(1000);
        queuedPlayLaunches = playLaunchService.getByStateAcrossTenants(LaunchState.Queued, null);
        Assert.assertEquals(queuedPlayLaunches.size(), 0);
        launchingPlayLaunches = playLaunchService.getByStateAcrossTenants(LaunchState.Launching, null);
        Assert.assertEquals(launchingPlayLaunches.size(), 2);

        // test that a play launch that's in queued state does not launch if
        // there already is a play launch launching to the same channel
        PlayLaunch playLaunch2a = playLaunchChannelService.createPlayLaunchFromChannel(playLaunchChannel2, play);
        Assert.assertNotNull(playLaunch2a);
        queuedPlayLaunches = playLaunchService.getByStateAcrossTenants(LaunchState.Queued, null);
        Assert.assertEquals(queuedPlayLaunches.size(), 1);
        launchingPlayLaunches = playLaunchService.getByStateAcrossTenants(LaunchState.Launching, null);
        Assert.assertEquals(launchingPlayLaunches.size(), 2);

        campaignLaunchTriggerService.triggerQueuedLaunches();
        Thread.sleep(1000);
        queuedPlayLaunches = playLaunchService.getByStateAcrossTenants(LaunchState.Queued, null);
        Assert.assertEquals(queuedPlayLaunches.size(), 1);
        launchingPlayLaunches = playLaunchService.getByStateAcrossTenants(LaunchState.Launching, null);
        Assert.assertEquals(launchingPlayLaunches.size(), 2);

    }

    @AfterClass(groups = "functional")
    public void teardown() throws Exception {
        if (playLaunch1 != null && playLaunch1.getLaunchId() != null) {
            playLaunchEntityMgr.deleteByLaunchId(playLaunch1.getLaunchId(), false);
        }
        if (playLaunch2 != null && playLaunch2.getLaunchId() != null) {
            playLaunchEntityMgr.deleteByLaunchId(playLaunch2.getLaunchId(), false);
        }
        Tenant tenant1 = tenantService.findByTenantId("testTenant1");
        if (tenant1 != null) {
            tenantService.discardTenant(tenant1);
        }
    }

    private PlayLaunchChannel createPlayLaunchChannel(Play play, LookupIdMap lookupIdMap) {
        PlayLaunchChannel playLaunchChannel = new PlayLaunchChannel();
        playLaunchChannel.setTenant(mainTestTenant);
        playLaunchChannel.setPlay(play);
        playLaunchChannel.setLookupIdMap(lookupIdMap);
        playLaunchChannel.setCreatedBy(CREATED_BY);
        playLaunchChannel.setUpdatedBy(CREATED_BY);
        playLaunchChannel.setLaunchType(LaunchType.FULL);
        playLaunchChannel.setLaunchUnscored(true);
        playLaunchChannel.setId(NamingUtils.randomSuffix("pl", 16));
        return playLaunchChannel;
    }
}