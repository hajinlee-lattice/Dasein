package com.latticeengines.apps.cdl.entitymgr.impl;

import java.time.Duration;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.apps.cdl.entitymgr.LookupIdMappingEntityMgr;
import com.latticeengines.apps.cdl.entitymgr.PlayEntityMgr;
import com.latticeengines.apps.cdl.entitymgr.PlayLaunchChannelEntityMgr;
import com.latticeengines.apps.cdl.entitymgr.PlayLaunchEntityMgr;
import com.latticeengines.apps.cdl.service.PlayTypeService;
import com.latticeengines.apps.cdl.testframework.CDLFunctionalTestNGBase;
import com.latticeengines.common.exposed.util.NamingUtils;
import com.latticeengines.domain.exposed.cdl.CDLExternalSystemName;
import com.latticeengines.domain.exposed.cdl.CDLExternalSystemType;
import com.latticeengines.domain.exposed.cdl.LaunchType;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.pls.LookupIdMap;
import com.latticeengines.domain.exposed.pls.Play;
import com.latticeengines.domain.exposed.pls.PlayLaunchChannel;
import com.latticeengines.domain.exposed.pls.PlayType;
import com.latticeengines.domain.exposed.pls.cdl.channel.MarketoChannelConfig;
import com.latticeengines.domain.exposed.pls.cdl.channel.OutreachChannelConfig;
import com.latticeengines.domain.exposed.pls.cdl.channel.SalesforceChannelConfig;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.security.exposed.service.TenantService;

public class PlayLaunchChannelEntityMgrImplTestNG extends CDLFunctionalTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(PlayLaunchChannelEntityMgrImplTestNG.class);

    @Autowired
    private PlayLaunchChannelEntityMgr playLaunchChannelEntityMgr;

    @Autowired
    private PlayEntityMgr playEntityMgr;

    @Autowired
    private LookupIdMappingEntityMgr lookupIdMappingEntityMgr;

    @Autowired
    private PlayLaunchEntityMgr playLaunchEntityMgr;

    @Autowired
    private PlayTypeService playTypeService;

    @Autowired
    private TenantService tenantService;

    private Play play;

    private LookupIdMap lookupIdMap1;
    private LookupIdMap lookupIdMap2;
    private LookupIdMap lookupIdMap3;
    private PlayLaunchChannel channel1;
    private PlayLaunchChannel channel2;
    private PlayLaunchChannel channel3;

    private List<PlayType> types;

    private String orgId1 = "org1";
    private String orgName1 = "salesforce_org";

    private String orgId2 = "org2";
    private String orgName2 = "marketo_org";

    private String orgId3 = "org3";
    private String orgName3 = "outreach_org";

    private long CURRENT_TIME_MILLIS = System.currentTimeMillis();

    private String NAME = "play" + CURRENT_TIME_MILLIS;
    private String DISPLAY_NAME = "play Harder";
    private String CREATED_BY = "lattice@lattice-engines.com";
    private String CRON_EXPRESSION = "0 0 12 ? * WED *";
    private static final long MAX_ACCOUNTS_TO_LAUNCH = 20L;
    private static final long NULL_MAX_ACCOUNTS_TO_LAUNCH = -1L;

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

        channel1 = createPlayLaunchChannel(play, lookupIdMap1);
        channel2 = createPlayLaunchChannel(play, lookupIdMap2);

        channel1.setIsAlwaysOn(true);
        channel2.setIsAlwaysOn(false);
        channel1.setChannelConfig(new SalesforceChannelConfig());

        MarketoChannelConfig config = new MarketoChannelConfig();
        config.setAudienceName("something");
        channel2.setChannelConfig(config);

        // Create Outreach
        lookupIdMap3 = new LookupIdMap();
        lookupIdMap3.setExternalSystemType(CDLExternalSystemType.MAP);
        lookupIdMap3.setExternalSystemName(CDLExternalSystemName.Outreach);
        lookupIdMap3.setOrgId(orgId3);
        lookupIdMap3.setOrgName(orgName3);

        lookupIdMap3 = lookupIdMappingEntityMgr.createExternalSystem(lookupIdMap3);
        Assert.assertNotNull(lookupIdMap3);

        channel3 = createPlayLaunchChannel(play, lookupIdMap3);
        OutreachChannelConfig outreachConfig = new OutreachChannelConfig();
        outreachConfig.setAudienceName("something");
        channel3.setChannelConfig(outreachConfig);
    }

    @Test(groups = "functional")
    public void testGetPreCreate() {
        List<PlayLaunchChannel> channels = playLaunchChannelEntityMgr.getAllValidScheduledChannels();
        Assert.assertNotNull(channels);
    }

    @Test(groups = "functional", dependsOnMethods = { "testGetPreCreate" })
    public void testNextDateFromCronExpression() {
        PlayLaunchChannel channel = new PlayLaunchChannel();
        channel.setCronScheduleExpression(CRON_EXPRESSION);
        Date d1 = PlayLaunchChannel.getNextDateFromCronExpression(channel);
        channel.setNextScheduledLaunch(
                Date.from(LocalDate.of(2019, 6, 14).atStartOfDay(ZoneId.systemDefault()).toInstant()));
        Date d2 = PlayLaunchChannel.getNextDateFromCronExpression(channel);
        Assert.assertEquals(d1, d2);
    }

    @Test(groups = "functional", dependsOnMethods = "testNextDateFromCronExpression")
    public void testFailingCreateChannel() {
        channel1.setCronScheduleExpression("");
        try {
            playLaunchChannelEntityMgr.createPlayLaunchChannel(channel1);
            Assert.fail("Should fail to create channel here");
        } catch (LedpException e) {
            Assert.assertEquals(e.getMessage(),
                    "Validation Error: Need a Cron Schedule Expression if a Channel is Always On");
        }

        channel1.setCronScheduleExpression("0 0 12 ? * WED *");
        try {
            playLaunchChannelEntityMgr.createPlayLaunchChannel(channel1);
            Assert.fail("Should fail to create channel here");
        } catch (LedpException e) {
            Assert.assertEquals(e.getMessage(),
                    "Validation Error: Need an expiration period if a Channel is Always On");
        }

        channel1.setExpirationPeriodString("Pasd");
        try {
            playLaunchChannelEntityMgr.createPlayLaunchChannel(channel1);
            Assert.fail("Should fail to create channel here");
        } catch (LedpException e) {
            Assert.assertEquals(e.getMessage(),
                    "Validation Error: Unable to parse the provided ExpirationPeriod: Pasd");
        }

        channel1.setExpirationPeriodString("P7M");
        try {
            playLaunchChannelEntityMgr.createPlayLaunchChannel(channel1);
            Assert.fail("Should fail to create channel here");
        } catch (LedpException e) {
            Assert.assertEquals(e.getCode(), LedpCode.LEDP_18232);
        }
    }

    @Test(groups = "functional", dependsOnMethods = "testFailingCreateChannel")
    public void testCreateChannel() throws InterruptedException {
        channel1.setExpirationPeriodString("P3M");
        playLaunchChannelEntityMgr.createPlayLaunchChannel(channel1);
        Assert.assertNotNull(channel1.getExpirationDate());
        Assert.assertNotNull(channel1.getNextScheduledLaunch());
        Assert.assertEquals(channel1.getExpirationPeriodString(), "P3M");

        Thread.sleep(1000);
        playLaunchChannelEntityMgr.createPlayLaunchChannel(channel2);
        Thread.sleep(1000);
        long playLaunchChannel1Pid = channel1.getPid();
        long playLaunchChannel2Pid = channel2.getPid();
        Assert.assertTrue(playLaunchChannel2Pid > playLaunchChannel1Pid);
        Assert.assertNotNull(channel1.getId());
        Assert.assertNotNull(channel2.getId());
    }

    @Test(groups = "functional", dependsOnMethods = { "testCreateChannel" })
    public void testBasicOperations() {

        PlayLaunchChannel retrieved = playLaunchChannelEntityMgr.findById(channel1.getId());
        Assert.assertNotNull(retrieved);
        Assert.assertEquals(retrieved.getId(), channel1.getId());

        retrieved = playLaunchChannelEntityMgr.findById(channel2.getId());
        Assert.assertNotNull(retrieved);
        Assert.assertEquals(retrieved.getId(), channel2.getId());

        List<PlayLaunchChannel> retrievedList = playLaunchChannelEntityMgr.findByIsAlwaysOnTrue();
        Assert.assertNotNull(retrievedList);
        Assert.assertEquals(retrievedList.size(), 1);

        retrieved = retrievedList.get(0);
        Assert.assertNotNull(retrieved);
        Assert.assertEquals(retrieved.getId(), channel1.getId());

        retrievedList = playLaunchChannelEntityMgr.findByPlayName(play.getName());
        Assert.assertNotNull(retrievedList);
        Assert.assertEquals(retrievedList.size(), 2);

        retrieved = playLaunchChannelEntityMgr.findByPlayNameAndLookupIdMapId(play.getName(), lookupIdMap1.getId());
        Assert.assertNotNull(retrieved);
        Assert.assertEquals(retrieved.getId(), channel1.getId());

        retrieved = playLaunchChannelEntityMgr.findByPlayNameAndLookupIdMapId(play.getName(), lookupIdMap2.getId());
        Assert.assertNotNull(retrieved);
        Assert.assertEquals(retrieved.getId(), channel2.getId());

    }

    @Test(groups = "functional", dependsOnMethods = { "testBasicOperations" })
    public void testExpirationAndScheduling() {
        PlayLaunchChannel retrieved = playLaunchChannelEntityMgr.findById(channel1.getId());

        Assert.assertNotNull(retrieved);
        Assert.assertEquals(retrieved.getId(), channel1.getId());
        Assert.assertTrue(retrieved.getIsAlwaysOn());
        Assert.assertNotNull(retrieved.getExpirationDate());

        channel1.setExpirationPeriodString("P4M");
        channel1.setCronScheduleExpression("0 0 12 ? * MON *");
        Date testDate = new Date();
        channel1.setExpirationDate(testDate);
        retrieved = playLaunchChannelEntityMgr.updatePlayLaunchChannel(retrieved, channel1);
        Assert.assertNotEquals(retrieved.getExpirationDate(), channel1.getExpirationDate());
        Assert.assertNotEquals(retrieved.getNextScheduledLaunch(), channel1.getNextScheduledLaunch());
        Assert.assertEquals(retrieved.getCronScheduleExpression(), channel1.getCronScheduleExpression());
        Assert.assertNotEquals(retrieved.getExpirationDate(), testDate);
        Assert.assertEquals(retrieved.getExpirationPeriodString(), "P4M");

        channel1.setIsAlwaysOn(false);
        retrieved = playLaunchChannelEntityMgr.updatePlayLaunchChannel(retrieved, channel1);
        Assert.assertNull(retrieved.getNextScheduledLaunch());
        Assert.assertNull(retrieved.getExpirationDate());
        channel1.setIsAlwaysOn(true);
        channel1.setCronScheduleExpression(CRON_EXPRESSION);
        retrieved = playLaunchChannelEntityMgr.updatePlayLaunchChannel(retrieved, channel1);
        Assert.assertNotNull(retrieved.getNextScheduledLaunch());
        Assert.assertNotNull(retrieved.getExpirationDate());

        channel1.setIsAlwaysOn(false);
        retrieved = playLaunchChannelEntityMgr.updatePlayLaunchChannel(retrieved, channel1);
        Assert.assertNull(retrieved.getExpirationDate());

        channel1.setIsAlwaysOn(true);
    }

    @Test(groups = "functional", dependsOnMethods = { "testExpirationAndScheduling" })
    public void testUpdate() throws InterruptedException {

        PlayLaunchChannel retrieved = playLaunchChannelEntityMgr.findById(channel1.getId());

        channel1.setExpirationPeriodString("P3W");
        retrieved = playLaunchChannelEntityMgr.updatePlayLaunchChannel(retrieved, channel1);
        Assert.assertNotNull(retrieved);
        Assert.assertEquals(retrieved.getId(), channel1.getId());
        Assert.assertEquals(retrieved.getExpirationPeriodString(), channel1.getExpirationPeriodString());

        channel1.setMaxAccountsToLaunch(NULL_MAX_ACCOUNTS_TO_LAUNCH);
        Assert.assertNotNull(channel1);
        Assert.assertNull(channel1.getMaxAccountsToLaunch());

        channel1.setIsAlwaysOn(false);
        retrieved = playLaunchChannelEntityMgr.updatePlayLaunchChannel(retrieved, channel1);
        Thread.sleep(1000);

        Assert.assertNotNull(retrieved);
        Assert.assertEquals(retrieved.getId(), channel1.getId());
        Assert.assertFalse(retrieved.getIsAlwaysOn());
        Assert.assertNull(retrieved.getExpirationDate());

        channel1.setLaunchUnscored(false);
        retrieved = playLaunchChannelEntityMgr.updatePlayLaunchChannel(retrieved, channel1);
        Thread.sleep(1000);
        Assert.assertNotNull(retrieved);
        Assert.assertEquals(retrieved.getId(), channel1.getId());
        Assert.assertFalse(retrieved.isLaunchUnscored());

        PlayLaunchChannel retrieved2 = playLaunchChannelEntityMgr.findById(channel2.getId());

        MarketoChannelConfig config = ((MarketoChannelConfig) channel2.getChannelConfig());
        config.setAudienceName("somethingElse");
        channel2.setChannelConfig(config);
        retrieved2 = playLaunchChannelEntityMgr.updatePlayLaunchChannel(retrieved2, channel2);
        Assert.assertNotNull(retrieved2);
        Assert.assertEquals(retrieved2.getId(), channel2.getId());
        Assert.assertEquals(((MarketoChannelConfig) retrieved2.getChannelConfig()).getAudienceName(), "somethingElse");
        Assert.assertTrue(retrieved2.getResetDeltaCalculationData());
    }

    @Test(groups = "functional", dependsOnMethods = { "testUpdate" })
    public void testDelete() {
        playLaunchChannelEntityMgr.deleteByChannelId(channel1.getId(), true);
        playLaunchChannelEntityMgr.deleteByChannelId(channel2.getId(), true);
    }

    @Test(groups = "functional", dependsOnMethods = { "testDelete" })
    public void testPostDelete() {
        try {
            Thread.sleep(TimeUnit.SECONDS.toMillis(10L));
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        PlayLaunchChannel retrieved = playLaunchChannelEntityMgr.findById(channel1.getId());
        Assert.assertNull(retrieved);
        retrieved = playLaunchChannelEntityMgr.findById(channel2.getId());
        Assert.assertNull(retrieved);
    }

    @AfterClass(groups = "functional")
    public void teardown() {
        if (channel1 != null && channel1.getId() != null) {
            playLaunchChannelEntityMgr.deleteByChannelId(channel1.getId(), true);
        }
        if (channel2 != null && channel2.getId() != null) {
            playLaunchChannelEntityMgr.deleteByChannelId(channel2.getId(), true);
        }
        Tenant tenant1 = tenantService.findByTenantId("testTenant1");
        if (tenant1 != null) {
            tenantService.discardTenant(tenant1);
        }
    }

    private PlayLaunchChannel createPlayLaunchChannel(Play play, LookupIdMap lookupIdMap) {
        PlayLaunchChannel channel = new PlayLaunchChannel();
        channel.setTenant(mainTestTenant);
        channel.setPlay(play);
        channel.setLookupIdMap(lookupIdMap);
        channel.setCreatedBy(CREATED_BY);
        channel.setUpdatedBy(CREATED_BY);
        channel.setLaunchType(LaunchType.FULL);
        channel.setId(NamingUtils.randomSuffix("pl", 16));
        channel.setMaxAccountsToLaunch(MAX_ACCOUNTS_TO_LAUNCH);
        channel.setExpirationDate(Date.from(new Date().toInstant().plus(Duration.ofHours(2))));
        return channel;
    }

}
