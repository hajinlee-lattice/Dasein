package com.latticeengines.apps.cdl.service.impl;

import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.springframework.retry.policy.SimpleRetryPolicy;
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
import com.latticeengines.apps.cdl.service.SegmentService;
import com.latticeengines.apps.cdl.testframework.CDLDeploymentTestNGBase;
import com.latticeengines.common.exposed.util.NamingUtils;
import com.latticeengines.domain.exposed.cdl.CDLExternalSystemName;
import com.latticeengines.domain.exposed.cdl.CDLExternalSystemType;
import com.latticeengines.domain.exposed.cdl.LaunchType;
import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.domain.exposed.pls.LaunchState;
import com.latticeengines.domain.exposed.pls.LookupIdMap;
import com.latticeengines.domain.exposed.pls.Play;
import com.latticeengines.domain.exposed.pls.PlayLaunch;
import com.latticeengines.domain.exposed.pls.PlayLaunchChannel;
import com.latticeengines.domain.exposed.pls.PlayType;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.security.exposed.service.TenantService;
import com.latticeengines.testframework.exposed.utils.TestRetryUtils;

public class CampaignLaunchTriggerServiceImplDeploymentTestNG extends CDLDeploymentTestNGBase {

    @Inject
    private PlayLaunchChannelEntityMgr playLaunchChannelEntityMgr;

    @Inject
    private PlayLaunchChannelService playLaunchChannelService;

    @Inject
    private CampaignLaunchTriggerService campaignLaunchTriggerService;

    @Inject
    private PlayEntityMgr playEntityMgr;

    @Inject
    private LookupIdMappingEntityMgr lookupIdMappingEntityMgr;

    @Inject
    private PlayLaunchEntityMgr playLaunchEntityMgr;

    @Inject
    private PlayLaunchService playLaunchService;

    @Inject
    private PlayTypeService playTypeService;

    @Inject
    private TenantService tenantService;

    @Inject
    private SegmentService segmentService;

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

    @BeforeClass(groups = "deployment")
    public void setup() throws Exception {
        setupTestEnvironment();

        MetadataSegment createdSegment = segmentService.createOrUpdateSegment(constructSegment(SEGMENT_NAME));
        MetadataSegment reTestSegment = segmentService.findByName(createdSegment.getName());

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
        play.setTargetSegment(reTestSegment);

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
        playLaunch1 = playLaunchChannelService.queueNewLaunchForChannel(play, playLaunchChannel1);
        Assert.assertNotNull(playLaunch1);
        playLaunch2 = playLaunchChannelService.queueNewLaunchForChannel(play, playLaunchChannel2);
        Assert.assertNotNull(playLaunch2);
        Thread.sleep(1000);

    }

    @Test(groups = "deployment", retryAnalyzer = SimpleRetryPolicy.class)
    public void testTriggerQueuedLaunches() throws InterruptedException {
        TestRetryUtils.retryForAssertionError(() -> {
            List<String> queuedPlayLaunchesIdList = //
                    playLaunchService.getByStateAcrossTenants(LaunchState.Queued, null) //
                            .stream().map(PlayLaunch::getId).collect(Collectors.toList());
            Assert.assertTrue(queuedPlayLaunchesIdList.contains(playLaunch1.getId()));
            Assert.assertTrue(queuedPlayLaunchesIdList.contains(playLaunch2.getId()));
        });
        TestRetryUtils.retryForAssertionError(() -> {
            List<String> launchingPlayLaunchesIdList = playLaunchService
                    .getByStateAcrossTenants(LaunchState.Launching, null).stream() //
                    .map(PlayLaunch::getId).collect(Collectors.toList());
            Assert.assertFalse(launchingPlayLaunchesIdList.contains(playLaunch1.getId()));
            Assert.assertFalse(launchingPlayLaunchesIdList.contains(playLaunch2.getId()));
        });

        campaignLaunchTriggerService.triggerQueuedLaunches();
        Thread.sleep(1000);
        TestRetryUtils.retryForAssertionError(() -> {
            List<String> queuedPlayLaunchesIdList = //
                    playLaunchService.getByStateAcrossTenants(LaunchState.Queued, null) //
                            .stream().map(PlayLaunch::getId).collect(Collectors.toList());
            Assert.assertFalse(queuedPlayLaunchesIdList.contains(playLaunch1.getId()));
            Assert.assertFalse(queuedPlayLaunchesIdList.contains(playLaunch2.getId()));
            List<String> launchingPlayLaunchesIdList = playLaunchService
                    .getByStateAcrossTenants(LaunchState.Launching, null).stream() //
                    .map(PlayLaunch::getId).collect(Collectors.toList());
            Assert.assertTrue(launchingPlayLaunchesIdList.contains(playLaunch1.getId()));
            Assert.assertTrue(launchingPlayLaunchesIdList.contains(playLaunch2.getId()));
        });

        // test that a play launch that's in queued state does not launch if
        // there already is a play launch launching to the same channel
        PlayLaunch playLaunch2a = playLaunchChannelService.queueNewLaunchForChannel(play, playLaunchChannel2);
        Assert.assertNotNull(playLaunch2a);
        Thread.sleep(1000);
        TestRetryUtils.retryForAssertionError(() -> {
            List<String> queuedPlayLaunchesIdList = playLaunchService.getByStateAcrossTenants(LaunchState.Queued, null)
                    .stream().map(PlayLaunch::getId).collect(Collectors.toList());
            Assert.assertTrue(queuedPlayLaunchesIdList.contains(playLaunch2a.getId()));
            List<String> launchingPlayLaunchesIdList = playLaunchService
                    .getByStateAcrossTenants(LaunchState.Launching, null).stream().map(PlayLaunch::getId)
                    .collect(Collectors.toList());
            Assert.assertFalse(launchingPlayLaunchesIdList.contains(playLaunch2a.getId()));
        });

        campaignLaunchTriggerService.triggerQueuedLaunches();
        Thread.sleep(1000);
        TestRetryUtils.retryForAssertionError(() -> {
            List<String> queuedPlayLaunchesIdList = //
                    playLaunchService.getByStateAcrossTenants(LaunchState.Queued, null) //
                            .stream().map(PlayLaunch::getId).collect(Collectors.toList());
            Assert.assertTrue(queuedPlayLaunchesIdList.contains(playLaunch2a.getId()));
            List<String> launchingPlayLaunchesIdList = playLaunchService
                    .getByStateAcrossTenants(LaunchState.Launching, null).stream() //
                    .map(PlayLaunch::getId).collect(Collectors.toList());
            Assert.assertFalse(launchingPlayLaunchesIdList.contains(playLaunch2a.getId()));
        });
    }

    @AfterClass(groups = "deployment")
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
