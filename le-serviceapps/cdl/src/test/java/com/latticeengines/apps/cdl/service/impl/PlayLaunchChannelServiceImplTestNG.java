package com.latticeengines.apps.cdl.service.impl;

import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.apps.cdl.entitymgr.LookupIdMappingEntityMgr;
import com.latticeengines.apps.cdl.service.PlayLaunchChannelService;
import com.latticeengines.apps.cdl.service.PlayLaunchService;
import com.latticeengines.apps.cdl.service.PlayService;
import com.latticeengines.apps.cdl.service.PlayTypeService;
import com.latticeengines.apps.cdl.testframework.CDLDeploymentTestNGBase;
import com.latticeengines.common.exposed.util.NamingUtils;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.cdl.CDLExternalSystemName;
import com.latticeengines.domain.exposed.cdl.CDLExternalSystemType;
import com.latticeengines.domain.exposed.cdl.LaunchType;
import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.domain.exposed.pls.LookupIdMap;
import com.latticeengines.domain.exposed.pls.Play;
import com.latticeengines.domain.exposed.pls.PlayLaunch;
import com.latticeengines.domain.exposed.pls.PlayLaunchChannel;
import com.latticeengines.domain.exposed.pls.PlayType;
import com.latticeengines.domain.exposed.pls.cdl.channel.MarketoChannelConfig;
import com.latticeengines.domain.exposed.pls.cdl.channel.SalesforceChannelConfig;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.proxy.exposed.cdl.SegmentProxy;
import com.latticeengines.proxy.exposed.pls.EmailProxy;
import com.latticeengines.security.exposed.service.TenantService;
public class PlayLaunchChannelServiceImplTestNG extends CDLDeploymentTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(PlayLaunchChannelServiceImplTestNG.class);

    @Inject
    private PlayLaunchChannelService playLaunchChannelService;

    @Inject
    private PlayLaunchService playLaunchService;

    @Inject
    private PlayService playService;

    @Inject
    private LookupIdMappingEntityMgr lookupIdMappingEntityMgr;

    @Inject
    private PlayTypeService playTypeService;

    @Inject
    private TenantService tenantService;

    @Inject
    private SegmentProxy segmentProxy;

    @Inject
    private EmailProxy emailProxy;

    private Play play;

    private LookupIdMap lookupIdMap1;
    private LookupIdMap lookupIdMap2;
    private LookupIdMap disconnectedLookupIdMap;
    private PlayLaunchChannel playLaunchChannel1;
    private PlayLaunchChannel playLaunchChannel2;
    private PlayLaunchChannel disconnectedPlayLaunchChannel;

    private List<PlayType> types;

    private String orgId1 = "org1";
    private String orgName1 = "salesforce_org";

    private String orgId2 = "org2";
    private String orgName2 = "marketo_org";

    private String orgId3 = "org3";
    private String orgName3 = "disconnected_org";

    private long CURRENT_TIME_MILLIS = System.currentTimeMillis();

    private String NAME = "play" + CURRENT_TIME_MILLIS;
    private String DISPLAY_NAME = "play Harder";
    private String CREATED_BY = "lattice@lattice-engines.com";
    private String UPDATED_BY = "pls-super-admin-tester@lattice-engines.com";
    private String CRON_EXPRESSION = "0 0 12 ? * WED *";
    private String current = "CURRENT";
    private String previous = "PREVIOUS";

    @BeforeClass(groups = "deployment-app")
    public void setup() throws Exception {
        setupTestEnvironment();
        MetadataSegment createdSegment = segmentProxy.createOrUpdateSegment(mainCustomerSpace,
                constructSegment(SEGMENT_NAME));
        Thread.sleep(1000);
        MetadataSegment retrievedSegment = segmentProxy.getMetadataSegmentByName(mainCustomerSpace,
                createdSegment.getName());
        Assert.assertNotNull(retrievedSegment);

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
        play.setTargetSegment(retrievedSegment);

        playService.createOrUpdate(play, mainTestTenant.getId());
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

        disconnectedLookupIdMap = new LookupIdMap();
        disconnectedLookupIdMap.setExternalSystemType(CDLExternalSystemType.MAP);
        disconnectedLookupIdMap.setExternalSystemName(CDLExternalSystemName.Marketo);
        disconnectedLookupIdMap.setOrgId(orgId3);
        disconnectedLookupIdMap.setOrgName(orgName3);
        disconnectedLookupIdMap = lookupIdMappingEntityMgr.createExternalSystem(disconnectedLookupIdMap);
        Assert.assertNotNull(lookupIdMap2);

        playLaunchChannel1 = createPlayLaunchChannel(play, lookupIdMap1);
        playLaunchChannel2 = createPlayLaunchChannel(play, lookupIdMap2);
        disconnectedPlayLaunchChannel = createPlayLaunchChannel(play, disconnectedLookupIdMap);

        playLaunchChannel1.setIsAlwaysOn(true);
        playLaunchChannel1.setExpirationPeriodString("P1W");
        playLaunchChannel1.setChannelConfig(new SalesforceChannelConfig());

        playLaunchChannel2.setIsAlwaysOn(false);
        playLaunchChannel2.setChannelConfig(new MarketoChannelConfig());

        createTable(current);
        createTable(previous);
    }

    @Test(groups = "deployment-app")
    public void testGetPreCreate() {
        List<PlayLaunchChannel> channels = playLaunchChannelService.getPlayLaunchChannels(play.getName(), false);
        Assert.assertNotNull(channels);
    }

    @Test(groups = "deployment-app", dependsOnMethods = "testGetPreCreate")
    public void testCreateChannel() throws InterruptedException {
        playLaunchChannelService.create(play.getName(), playLaunchChannel1);
        Thread.sleep(1000);
        playLaunchChannelService.create(play.getName(), playLaunchChannel2);
        playLaunchChannelService.createNewLaunchByPlayAndChannel(play, playLaunchChannel2, null, false);
        Thread.sleep(1000);
        long playLaunchChannel1Pid = playLaunchChannel1.getPid();
        long playLaunchChannel2Pid = playLaunchChannel2.getPid();
        Assert.assertTrue(playLaunchChannel2Pid > playLaunchChannel1Pid);
        Assert.assertNotNull(playLaunchChannel1.getId());
        Assert.assertNotNull(playLaunchChannel2.getId());
    }

    @Test(groups = "deployment-app", dependsOnMethods = "testCreateChannel")
    public void testBasicOperations() {
        LookupIdMap disconnectedMap = lookupIdMappingEntityMgr.getLookupIdMap(disconnectedLookupIdMap.getId());
        disconnectedMap.setIsRegistered(false);
        lookupIdMappingEntityMgr.updateLookupIdMap(disconnectedMap);

        List<PlayLaunchChannel> channelList = playLaunchChannelService.getPlayLaunchChannels(play.getName(), true);
        Assert.assertFalse(channelList.contains(disconnectedPlayLaunchChannel));

        PlayLaunchChannel retrieved = playLaunchChannelService.findById(playLaunchChannel1.getId());
        Assert.assertNotNull(retrieved);
        Assert.assertEquals(retrieved.getId(), playLaunchChannel1.getId());

        retrieved = playLaunchChannelService.findById(playLaunchChannel2.getId());
        Assert.assertNotNull(retrieved);
        Assert.assertEquals(retrieved.getId(), playLaunchChannel2.getId());

        List<PlayLaunchChannel> retrievedList = playLaunchChannelService.findByIsAlwaysOnTrue();
        Assert.assertNotNull(retrievedList);
        Assert.assertEquals(retrievedList.size(), 1);

        retrieved = retrievedList.get(0);
        Assert.assertNotNull(retrieved);
        Assert.assertEquals(retrieved.getId(), playLaunchChannel1.getId());

        retrievedList = playLaunchChannelService.getPlayLaunchChannels(play.getName(), false);
        Assert.assertNotNull(retrievedList);
        Assert.assertEquals(retrievedList.size(), 2);

        retrieved = playLaunchChannelService.findByPlayNameAndLookupIdMapId(play.getName(), lookupIdMap1.getId());
        Assert.assertNotNull(retrieved);
        Assert.assertEquals(retrieved.getId(), playLaunchChannel1.getId());

        retrieved = playLaunchChannelService.findByPlayNameAndLookupIdMapId(play.getName(), lookupIdMap2.getId());
        Assert.assertNotNull(retrieved);
        Assert.assertEquals(retrieved.getId(), playLaunchChannel2.getId());

    }

    @Test(groups = "deployment-app", dependsOnMethods = "testBasicOperations")
    public void testCreateFromChannel() {
        PlayLaunch retrievedLaunch = playLaunchService.findLatestByChannel(playLaunchChannel1.getPid());
        Assert.assertNull(retrievedLaunch);

        retrievedLaunch = playLaunchService.findLatestByChannel(playLaunchChannel2.getPid());
        playLaunchChannel2 = playLaunchChannelService.findById(playLaunchChannel2.getId());
        Assert.assertNotNull(retrievedLaunch);
        Assert.assertNotNull(playLaunchChannel2.getLastLaunch());
        Assert.assertEquals(playLaunchChannel2.getLastLaunch().getId(), retrievedLaunch.getId());

    }

    @Test(groups = "deployment-app", dependsOnMethods = { "testCreateFromChannel" })
    public void testEmailSentOnSecondToLastLaunch() throws InterruptedException {
        playLaunchChannelService.updateNextScheduledDate(play.getName(), playLaunchChannel1.getId());
        Assert.assertTrue(emailProxy.sendPlayLaunchChannelExpiringEmail(MultiTenantContext.getTenant().getId(),
                playLaunchChannel1));
    }

    @Test(groups = "deployment-app", dependsOnMethods = { "testEmailSentOnSecondToLastLaunch" })
    public void testUpdate() throws InterruptedException {
        playLaunchChannel1.setIsAlwaysOn(false);
        PlayLaunchChannel retrieved = playLaunchChannelService.update(play.getName(), playLaunchChannel1);
        Thread.sleep(1000);

        Assert.assertNotNull(retrieved);
        Assert.assertEquals(retrieved.getId(), playLaunchChannel1.getId());
        Assert.assertFalse(retrieved.getIsAlwaysOn());
        Assert.assertNull(retrieved.getExpirationDate());
    }

    @Test(groups = "deployment-app", dependsOnMethods = { "testUpdate" })
    public void testRecoverLaunchUniverses() throws InterruptedException {
        playLaunchChannel1.setCurrentLaunchedAccountUniverseTable(current);
        playLaunchChannel1.setPreviousLaunchedAccountUniverseTable(previous);

        PlayLaunchChannel retrievedChannel = playLaunchChannelService.recoverLaunchUniverse(playLaunchChannel1);

        Assert.assertNotNull(retrievedChannel);
        Assert.assertEquals(retrievedChannel.getCurrentLaunchedAccountUniverseTable(), previous);
        Assert.assertNull(retrievedChannel.getPreviousLaunchedAccountUniverseTable());
        Assert.assertNull(retrievedChannel.getCurrentLaunchedContactUniverseTable());
        Assert.assertNull(retrievedChannel.getPreviousLaunchedContactUniverseTable());

        playLaunchChannel1.setCurrentLaunchedContactUniverseTable(current);
        playLaunchChannel1.setPreviousLaunchedContactUniverseTable(previous);

        retrievedChannel = playLaunchChannelService.recoverLaunchUniverse(playLaunchChannel1);

        Assert.assertNotNull(retrievedChannel);
        Assert.assertEquals(retrievedChannel.getCurrentLaunchedAccountUniverseTable(), previous);
        Assert.assertNull(retrievedChannel.getPreviousLaunchedAccountUniverseTable());
        Assert.assertEquals(retrievedChannel.getCurrentLaunchedContactUniverseTable(), previous);
        Assert.assertNull(retrievedChannel.getPreviousLaunchedContactUniverseTable());

        // This should not go through b/c UI has access to this call and lazily
        // populates data
        playLaunchChannel1.setCurrentLaunchedAccountUniverseTable(null);
        playLaunchChannel1.setPreviousLaunchedAccountUniverseTable(null);
        playLaunchChannel1.setCurrentLaunchedContactUniverseTable(null);
        playLaunchChannel1.setPreviousLaunchedContactUniverseTable(null);
        retrievedChannel = playLaunchChannelService.update(play.getName(), playLaunchChannel1);

        Assert.assertEquals(retrievedChannel.getCurrentLaunchedAccountUniverseTable(), previous);
        Assert.assertEquals(retrievedChannel.getCurrentLaunchedContactUniverseTable(), previous);

        retrievedChannel = playLaunchChannelService.recoverLaunchUniverse(playLaunchChannel1);
        Assert.assertNotNull(retrievedChannel);
        Assert.assertNull(retrievedChannel.getCurrentLaunchedAccountUniverseTable());
        Assert.assertNull(retrievedChannel.getPreviousLaunchedAccountUniverseTable());
        Assert.assertNull(retrievedChannel.getCurrentLaunchedContactUniverseTable());
        Assert.assertNull(retrievedChannel.getPreviousLaunchedContactUniverseTable());
    }

    @Test(groups = "deployment-app", dependsOnMethods = { "testRecoverLaunchUniverses" })
    public void testDelete() {
        playLaunchChannelService.deleteByChannelId(playLaunchChannel1.getId(), true);
        playLaunchChannelService.deleteByChannelId(playLaunchChannel2.getId(), true);
    }

    @Test(groups = "deployment-app", dependsOnMethods = { "testDelete" })
    public void testPostDelete() {
        try {
            Thread.sleep(TimeUnit.SECONDS.toMillis(10L));
        } catch (InterruptedException e) {
            log.error("Sleep Inturrupted: " + e.getMessage());
        }
        PlayLaunchChannel retrieved = playLaunchChannelService.findById(playLaunchChannel1.getId());
        Assert.assertNull(retrieved);
        retrieved = playLaunchChannelService.findById(playLaunchChannel2.getId());
        Assert.assertNull(retrieved);
    }

    @AfterClass(groups = "deployment-app")
    public void teardown() {
        if (playLaunchChannel1 != null && playLaunchChannel1.getId() != null) {
            playLaunchChannelService.deleteByChannelId(playLaunchChannel1.getId(), true);
        }
        if (playLaunchChannel2 != null && playLaunchChannel2.getId() != null) {
            playLaunchChannelService.deleteByChannelId(playLaunchChannel2.getId(), true);
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
        playLaunchChannel.setUpdatedBy(UPDATED_BY);
        playLaunchChannel.setLaunchType(LaunchType.FULL);
        playLaunchChannel.setId(NamingUtils.randomSuffix("pl", 16));
        playLaunchChannel.setLaunchUnscored(false);
        playLaunchChannel.setCronScheduleExpression(CRON_EXPRESSION);
        return playLaunchChannel;
    }

}
