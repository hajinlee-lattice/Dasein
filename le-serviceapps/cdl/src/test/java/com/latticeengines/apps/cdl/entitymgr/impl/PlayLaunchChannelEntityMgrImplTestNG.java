package com.latticeengines.apps.cdl.entitymgr.impl;

import java.time.Duration;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.retry.support.RetryTemplate;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.apps.cdl.entitymgr.LookupIdMappingEntityMgr;
import com.latticeengines.apps.cdl.entitymgr.PlayEntityMgr;
import com.latticeengines.apps.cdl.entitymgr.PlayLaunchChannelEntityMgr;
import com.latticeengines.apps.cdl.service.PlayTypeService;
import com.latticeengines.apps.cdl.testframework.CDLFunctionalTestNGBase;
import com.latticeengines.common.exposed.util.NamingUtils;
import com.latticeengines.common.exposed.util.RetryUtils;
import com.latticeengines.common.exposed.util.SleepUtils;
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
import com.latticeengines.domain.exposed.pls.cdl.channel.S3ChannelConfig;
import com.latticeengines.domain.exposed.pls.cdl.channel.SalesforceChannelConfig;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.util.AttributeUtils;
import com.latticeengines.security.exposed.service.TenantService;
public class PlayLaunchChannelEntityMgrImplTestNG extends CDLFunctionalTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(PlayLaunchChannelEntityMgrImplTestNG.class);

    @Inject
    private PlayLaunchChannelEntityMgr playLaunchChannelEntityMgr;

    @Inject
    private PlayEntityMgr playEntityMgr;

    @Inject
    private LookupIdMappingEntityMgr lookupIdMappingEntityMgr;

    @Inject
    private PlayTypeService playTypeService;

    @Inject
    private TenantService tenantService;

    private Play play;

    private LookupIdMap lookupIdMap1;
    private LookupIdMap lookupIdMap2;
    private LookupIdMap lookupIdMap3;
    private LookupIdMap lookupIdMap4;
    private PlayLaunchChannel channel1;
    private PlayLaunchChannel channel2;
    private PlayLaunchChannel channel3;
    private PlayLaunchChannel channel4;

    private List<PlayType> types;

    private String orgId1 = "org1";
    private String orgName1 = "salesforce_org";

    private String orgId2 = "org2";
    private String orgName2 = "marketo_org";

    private String orgId3 = "org3";
    private String orgName3 = "outreach_org";

    private String orgId4 = "org4";
    private String orgName4 = "s3_org";

    private String tableName1 = "table1";
    private String tableName2 = "table2";
    private String tableName3 = "table3";
    private String tableName4 = "table4";
    private String tableNameDoesntExist = "tableNameDoesntExist";

    private long CURRENT_TIME_MILLIS = System.currentTimeMillis();

    private String NAME = "play" + CURRENT_TIME_MILLIS;
    private String DISPLAY_NAME = "play Harder";
    private String CREATED_BY = "lattice@lattice-engines.com";
    private String cron = "0 0 12 ? * WED *";
    private static final long MAX_ACCOUNTS_TO_LAUNCH = 20L;
    private static final long NULL_MAX_ACCOUNTS_TO_LAUNCH = -1L;
    private static final long NULL_MAX_CONTACTS_PER_ACCOUNT = -1L;

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

        lookupIdMap1 = createLookupIdMap(CDLExternalSystemType.CRM, CDLExternalSystemName.Salesforce, orgId1, orgName1);
        Assert.assertNotNull(lookupIdMap1);
        channel1 = createPlayLaunchChannel(play, lookupIdMap1);
        channel1.setIsAlwaysOn(true);
        channel1.setChannelConfig(new SalesforceChannelConfig());

        lookupIdMap2 = createLookupIdMap(CDLExternalSystemType.MAP, CDLExternalSystemName.Marketo, orgId2, orgName2);
        Assert.assertNotNull(lookupIdMap2);
        channel2 = createPlayLaunchChannel(play, lookupIdMap2);
        channel2.setIsAlwaysOn(false);
        MarketoChannelConfig config = new MarketoChannelConfig();
        config.setAudienceName("something");
        channel2.setChannelConfig(config);

        // Create Outreach
        lookupIdMap3 = createLookupIdMap(CDLExternalSystemType.MAP, CDLExternalSystemName.Outreach, orgId3, orgName3);
        Assert.assertNotNull(lookupIdMap3);
        channel3 = createPlayLaunchChannel(play, lookupIdMap3);
        OutreachChannelConfig outreachConfig = new OutreachChannelConfig();
        outreachConfig.setAudienceName("something");
        channel3.setChannelConfig(outreachConfig);

        // Create S3
        lookupIdMap4 = createLookupIdMap(CDLExternalSystemType.FILE_SYSTEM, CDLExternalSystemName.AWS_S3, orgId4, orgName4);
        Assert.assertNotNull(lookupIdMap4);
        channel4 = createPlayLaunchChannel(play, lookupIdMap4);
        S3ChannelConfig s3ChannelConfig = new S3ChannelConfig();
        s3ChannelConfig.setAudienceName("something");
        s3ChannelConfig.setAttributeSetName("attribute_set_name_s3");
        s3ChannelConfig.setAddExportTimestamp(true);
        channel4.setIsAlwaysOn(true);
        channel4.setCronScheduleExpression("0 0 12 ? * WED *");
        channel4.setExpirationPeriodString("P3M");
        channel4.setChannelConfig(s3ChannelConfig);

        // Create tables
        createTable(tableName1);
        createTable(tableName2);
        createTable(tableName3);
        createTable(tableName4);
    }

    private LookupIdMap createLookupIdMap(CDLExternalSystemType externalSystemType, CDLExternalSystemName externalSystemName, String orgId, String orgName) {
        LookupIdMap lookupIdMap = new LookupIdMap();
        lookupIdMap.setExternalSystemType(externalSystemType);
        lookupIdMap.setExternalSystemName(externalSystemName);
        lookupIdMap.setOrgId(orgId);
        lookupIdMap.setOrgName(orgName);
        return lookupIdMappingEntityMgr.createExternalSystem(lookupIdMap);
    }

    @Test(groups = "functional")
    public void testGetPreCreate() {
        List<PlayLaunchChannel> channels = playLaunchChannelEntityMgr.getAllValidScheduledChannels();
        Assert.assertNotNull(channels);
    }

    @Test(groups = "functional", dependsOnMethods = { "testGetPreCreate" })
    public void testNextDateFromCronExpression() {
        String cronExpression = "0 45 12 ? * WED *";
        PlayLaunchChannel channel = new PlayLaunchChannel();
        channel.setCronScheduleExpression(cronExpression);
        Date d1 = PlayLaunchChannel.getNextDateFromCronExpression(channel);
        channel.setNextScheduledLaunch(Date.from(LocalDate.of(2019, 6, 14).atStartOfDay(ZoneOffset.UTC).toInstant()));
        Date d2 = PlayLaunchChannel.getNextDateFromCronExpression(channel);
        Assert.assertEquals(d1, d2);

        channel.setNextScheduledLaunch(d2);
        Date d3 = PlayLaunchChannel.getNextDateFromCronExpression(channel);
        Assert.assertEquals(d2, d3);
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

        playLaunchChannelEntityMgr.createPlayLaunchChannel(channel4);
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
        Assert.assertEquals(retrievedList.size(), 2);

        retrieved = retrievedList.get(0);
        Assert.assertNotNull(retrieved);
        Assert.assertEquals(retrieved.getId(), channel1.getId());

        retrievedList = playLaunchChannelEntityMgr.findByPlayName(play.getName());
        Assert.assertNotNull(retrievedList);
        Assert.assertEquals(retrievedList.size(), 3);

        retrieved = playLaunchChannelEntityMgr.findByPlayNameAndLookupIdMapId(play.getName(), lookupIdMap1.getId());
        Assert.assertNotNull(retrieved);
        Assert.assertEquals(retrieved.getId(), channel1.getId());

        retrieved = playLaunchChannelEntityMgr.findByPlayNameAndLookupIdMapId(play.getName(), lookupIdMap2.getId());
        Assert.assertNotNull(retrieved);
        Assert.assertEquals(retrieved.getId(), channel2.getId());

        List<Play> plays = playEntityMgr.findByAlwaysOnAndAttrSetName("attribute_set_name_s3");
        Assert.assertEquals(plays.size(), 1);
        plays = playEntityMgr.findByAlwaysOnAndAttrSetName("test_attribute_set_name_s3");
        Assert.assertEquals(plays.size(), 0);
        channel4.setIsAlwaysOn(false);
        playLaunchChannelEntityMgr.update(channel4);
        RetryTemplate retry = RetryUtils.getRetryTemplate(10, Collections.singleton(AssertionError.class), null);
        AtomicReference<PlayLaunchChannel> updatedChannel = new AtomicReference<>();
        retry.execute(context -> {
            updatedChannel.set(playLaunchChannelEntityMgr.findById(channel4.getId()));
            Assert.assertNotNull(updatedChannel.get());
            Assert.assertFalse(updatedChannel.get().getIsAlwaysOn());
            return true;
        });
        playLaunchChannelEntityMgr.updateAttributeSetNameToDefault("attribute_set_name_s3");
        retry = RetryUtils.getRetryTemplate(10, Collections.singleton(AssertionError.class), null);
        retry.execute(context -> {
            updatedChannel.set(playLaunchChannelEntityMgr.findById(channel4.getId()));
            Assert.assertNotNull(updatedChannel.get());
            log.info("Channel4 id is {}.", channel4.getId());
            Assert.assertEquals(((S3ChannelConfig) updatedChannel.get().getChannelConfig()).getAttributeSetName(), AttributeUtils.DEFAULT_ATTRIBUTE_SET_NAME);
            Assert.assertEquals(((S3ChannelConfig) updatedChannel.get().getChannelConfig()).getAddExportTimestamp(), true);
            return true;
        });
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
        channel1.setCronScheduleExpression(cron);
        retrieved = playLaunchChannelEntityMgr.updatePlayLaunchChannel(retrieved, channel1);
        Assert.assertNotNull(retrieved.getCronScheduleExpression());
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

        channel1.setMaxEntitiesToLaunch(NULL_MAX_ACCOUNTS_TO_LAUNCH);
        channel1.setMaxContactsPerAccount(NULL_MAX_CONTACTS_PER_ACCOUNT);
        Assert.assertNotNull(channel1);

        channel1.setIsAlwaysOn(false);
        retrieved = playLaunchChannelEntityMgr.updatePlayLaunchChannel(retrieved, channel1);
        Thread.sleep(1000);

        Assert.assertNotNull(retrieved);
        Assert.assertEquals(retrieved.getId(), channel1.getId());
        Assert.assertFalse(retrieved.getIsAlwaysOn());
        Assert.assertNull(retrieved.getExpirationDate());
        Assert.assertNull(retrieved.getMaxEntitiesToLaunch());
        Assert.assertNull(retrieved.getMaxContactsPerAccount());

        channel1.setLaunchUnscored(false);
        retrieved = playLaunchChannelEntityMgr.updatePlayLaunchChannel(retrieved, channel1);
        Thread.sleep(1000);
        Assert.assertNotNull(retrieved);
        Assert.assertEquals(retrieved.getId(), channel1.getId());
        Assert.assertFalse(retrieved.getLaunchUnscored());

        PlayLaunchChannel retrieved2 = playLaunchChannelEntityMgr.findById(channel2.getId());

        MarketoChannelConfig config = ((MarketoChannelConfig) channel2.getChannelConfig());
        config.setAudienceName("somethingElse");
        channel2.setChannelConfig(config);
        channel2.setMaxContactsPerAccount(5L);
        retrieved2 = playLaunchChannelEntityMgr.updatePlayLaunchChannel(retrieved2, channel2);
        Assert.assertNotNull(retrieved2);
        Assert.assertEquals(retrieved2.getId(), channel2.getId());
        Assert.assertEquals(retrieved2.getChannelConfig().getAudienceName(), "somethingElse");
        Assert.assertTrue(retrieved2.getResetDeltaCalculationData());
        Assert.assertEquals(retrieved2.getMaxContactsPerAccount(), channel2.getMaxContactsPerAccount());
    }

    @Test(groups = "functional", dependsOnMethods = { "testUpdate" })
    public void testCannotUpdateCurrentTablesToNull() {
        PlayLaunchChannel retrieved = playLaunchChannelEntityMgr.findById(channel1.getId());
        Assert.assertNull(retrieved.getCurrentLaunchedAccountUniverseTable());
        Assert.assertNull(retrieved.getCurrentLaunchedContactUniverseTable());
        Assert.assertNull(retrieved.getPreviousLaunchedAccountUniverseTable());
        Assert.assertNull(retrieved.getPreviousLaunchedContactUniverseTable());

        channel1.setCurrentLaunchedAccountUniverseTable(tableName1);
        retrieved = playLaunchChannelEntityMgr.updatePlayLaunchChannel(retrieved, channel1);
        Assert.assertNotNull(retrieved);
        Assert.assertEquals(retrieved.getCurrentLaunchedAccountUniverseTable(), tableName1);
        Assert.assertNull(retrieved.getCurrentLaunchedContactUniverseTable());
        Assert.assertNull(retrieved.getPreviousLaunchedAccountUniverseTable());
        Assert.assertNull(retrieved.getPreviousLaunchedContactUniverseTable());

        channel1.setCurrentLaunchedContactUniverseTable(tableName2);
        retrieved = playLaunchChannelEntityMgr.updatePlayLaunchChannel(retrieved, channel1);
        Assert.assertNotNull(retrieved);
        Assert.assertEquals(retrieved.getCurrentLaunchedAccountUniverseTable(), tableName1);
        Assert.assertEquals(retrieved.getCurrentLaunchedContactUniverseTable(), tableName2);
        Assert.assertNull(retrieved.getPreviousLaunchedAccountUniverseTable());
        Assert.assertNull(retrieved.getPreviousLaunchedContactUniverseTable());

        channel1.setPreviousLaunchedAccountUniverseTable(tableName3);
        channel1.setPreviousLaunchedContactUniverseTable(tableName4);
        retrieved = playLaunchChannelEntityMgr.updatePlayLaunchChannel(retrieved, channel1);
        Assert.assertNotNull(retrieved);
        Assert.assertEquals(retrieved.getCurrentLaunchedAccountUniverseTable(), tableName1);
        Assert.assertEquals(retrieved.getCurrentLaunchedContactUniverseTable(), tableName2);
        Assert.assertNull(retrieved.getPreviousLaunchedAccountUniverseTable());
        Assert.assertNull(retrieved.getPreviousLaunchedContactUniverseTable());

        channel1.setCurrentLaunchedAccountUniverseTable(tableName3);
        channel1.setCurrentLaunchedContactUniverseTable(tableName4);
        retrieved = playLaunchChannelEntityMgr.updatePlayLaunchChannel(retrieved, channel1);
        Assert.assertNotNull(retrieved);
        Assert.assertEquals(retrieved.getCurrentLaunchedAccountUniverseTable(), tableName3);
        Assert.assertEquals(retrieved.getCurrentLaunchedContactUniverseTable(), tableName4);
        Assert.assertNull(retrieved.getPreviousLaunchedAccountUniverseTable());
        Assert.assertNull(retrieved.getPreviousLaunchedContactUniverseTable());

        channel1.setCurrentLaunchedAccountUniverseTable(null);
        retrieved = playLaunchChannelEntityMgr.updatePlayLaunchChannel(retrieved, channel1);
        Assert.assertNotNull(retrieved);
        Assert.assertEquals(retrieved.getCurrentLaunchedAccountUniverseTable(), tableName3);
        Assert.assertEquals(retrieved.getCurrentLaunchedContactUniverseTable(), tableName4);
        Assert.assertNull(retrieved.getPreviousLaunchedAccountUniverseTable());
        Assert.assertNull(retrieved.getPreviousLaunchedContactUniverseTable());

        channel1.setCurrentLaunchedContactUniverseTable(null);
        retrieved = playLaunchChannelEntityMgr.updatePlayLaunchChannel(retrieved, channel1);
        Assert.assertNotNull(retrieved);
        Assert.assertEquals(retrieved.getCurrentLaunchedAccountUniverseTable(), tableName3);
        Assert.assertEquals(retrieved.getCurrentLaunchedContactUniverseTable(), tableName4);
        Assert.assertNull(retrieved.getPreviousLaunchedAccountUniverseTable());
        Assert.assertNull(retrieved.getPreviousLaunchedContactUniverseTable());

        try {
            channel1.setCurrentLaunchedAccountUniverseTable(tableNameDoesntExist);
            playLaunchChannelEntityMgr.updatePlayLaunchChannel(retrieved, channel1);
        } catch (LedpException e) {
            Assert.assertEquals(e.getCode(), LedpCode.LEDP_32000);
        }
    }

    @Test(groups = "functional", dependsOnMethods = { "testCannotUpdateCurrentTablesToNull" })
    public void testDelete() {
        playLaunchChannelEntityMgr.deleteByChannelId(channel1.getId(), true);
        playLaunchChannelEntityMgr.deleteByChannelId(channel2.getId(), true);
    }

    @Test(groups = "functional", dependsOnMethods = { "testDelete" })
    public void testPostDelete() {
        SleepUtils.sleep(TimeUnit.SECONDS.toMillis(10L));
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
        if (channel4 != null && channel4.getId() != null) {
            playLaunchChannelEntityMgr.deleteByChannelId(channel4.getId(), true);
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
        channel.setMaxEntitiesToLaunch(MAX_ACCOUNTS_TO_LAUNCH);
        channel.setLaunchUnscored(false);
        channel.setExpirationDate(Date.from(new Date().toInstant().plus(Duration.ofHours(2))));
        return channel;
    }

}
