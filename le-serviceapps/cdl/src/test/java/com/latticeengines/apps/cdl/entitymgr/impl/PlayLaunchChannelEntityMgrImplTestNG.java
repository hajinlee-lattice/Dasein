package com.latticeengines.apps.cdl.entitymgr.impl;

import java.text.ParseException;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.core.util.CronExpression;
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
import com.latticeengines.domain.exposed.pls.LookupIdMap;
import com.latticeengines.domain.exposed.pls.Play;
import com.latticeengines.domain.exposed.pls.PlayLaunchChannel;
import com.latticeengines.domain.exposed.pls.PlayType;
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
    private PlayLaunchChannel playLaunchChannel1;
    private PlayLaunchChannel playLaunchChannel2;

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
        playLaunchChannel2.setIsAlwaysOn(false);

    }

    @Test(groups = "functional")
    public void testGetPreCreate() {
        List<PlayLaunchChannel> channels = playLaunchChannelEntityMgr.getAllValidScheduledChannels();
        Assert.assertNotNull(channels);
    }

    @Test(groups = "functional", dependsOnMethods = { "testGetPreCreate" })
    public void testNextDateFromCronExpression() throws ParseException {
        CronExpression cronExp = new CronExpression("0 0 12 ? * WED *");
        PlayLaunchChannel channel = new PlayLaunchChannel();
        channel.setCronScheduleExpression("0 0 12 ? * WED *");

        Date d1 = PlayLaunchChannel.getNextDateFromCronExpression(channel);
        channel.setNextScheduledLaunch(
                Date.from(LocalDate.of(2019, 6, 14).atStartOfDay(ZoneId.systemDefault()).toInstant()));

        Date d2 = PlayLaunchChannel.getNextDateFromCronExpression(channel);

        Assert.assertEquals(d1, d2);
    }

    @Test(groups = "functional", dependsOnMethods = { "testNextDateFromCronExpression" })
    public void testCreateChannel() throws InterruptedException {
        playLaunchChannelEntityMgr.create(playLaunchChannel1);
        Thread.sleep(1000);
        playLaunchChannelEntityMgr.create(playLaunchChannel2);
        Thread.sleep(1000);
        long playLaunchChannel1Pid = playLaunchChannel1.getPid();
        long playLaunchChannel2Pid = playLaunchChannel2.getPid();
        Assert.assertTrue(playLaunchChannel2Pid > playLaunchChannel1Pid);
        Assert.assertNotNull(playLaunchChannel1.getId());
        Assert.assertNotNull(playLaunchChannel2.getId());
    }

    @Test(groups = "functional", dependsOnMethods = { "testCreateChannel" })
    public void testBasicOperations() {

        PlayLaunchChannel retrieved = playLaunchChannelEntityMgr.findById(playLaunchChannel1.getId());
        Assert.assertNotNull(retrieved);
        Assert.assertEquals(retrieved.getId(), playLaunchChannel1.getId());

        retrieved = playLaunchChannelEntityMgr.findById(playLaunchChannel2.getId());
        Assert.assertNotNull(retrieved);
        Assert.assertEquals(retrieved.getId(), playLaunchChannel2.getId());

        List<PlayLaunchChannel> retrievedList = playLaunchChannelEntityMgr.findByIsAlwaysOnTrue();
        Assert.assertNotNull(retrievedList);
        Assert.assertEquals(retrievedList.size(), 1);

        retrieved = retrievedList.get(0);
        Assert.assertNotNull(retrieved);
        Assert.assertEquals(retrieved.getId(), playLaunchChannel1.getId());

        retrievedList = playLaunchChannelEntityMgr.findByPlayName(play.getName());
        Assert.assertNotNull(retrievedList);
        Assert.assertEquals(retrievedList.size(), 2);

        retrieved = playLaunchChannelEntityMgr.findByPlayNameAndLookupIdMapId(play.getName(), lookupIdMap1.getId());
        Assert.assertNotNull(retrieved);
        Assert.assertEquals(retrieved.getId(), playLaunchChannel1.getId());

        retrieved = playLaunchChannelEntityMgr.findByPlayNameAndLookupIdMapId(play.getName(), lookupIdMap2.getId());
        Assert.assertNotNull(retrieved);
        Assert.assertEquals(retrieved.getId(), playLaunchChannel2.getId());

    }

    @Test(groups = "functional", dependsOnMethods = { "testBasicOperations" })
    public void testUpdate() throws InterruptedException {

        PlayLaunchChannel retrieved = playLaunchChannelEntityMgr.findById(playLaunchChannel1.getId());
        playLaunchChannel1.setIsAlwaysOn(false);
        retrieved = playLaunchChannelEntityMgr.updatePlayLaunchChannel(retrieved, playLaunchChannel1);
        Thread.sleep(1000);

        Assert.assertNotNull(retrieved);
        Assert.assertEquals(retrieved.getId(), playLaunchChannel1.getId());
        Assert.assertFalse(retrieved.getIsAlwaysOn());

    }

    @Test(groups = "functional", dependsOnMethods = { "testUpdate" })
    public void testDelete() {
        playLaunchChannelEntityMgr.deleteByChannelId(playLaunchChannel1.getId(), true);
        playLaunchChannelEntityMgr.deleteByChannelId(playLaunchChannel2.getId(), true);
    }

    @Test(groups = "functional", dependsOnMethods = { "testDelete" })
    public void testPostDelete() {
        try {
            Thread.sleep(TimeUnit.SECONDS.toMillis(10L));
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        PlayLaunchChannel retrieved = playLaunchChannelEntityMgr.findById(playLaunchChannel1.getId());
        Assert.assertNull(retrieved);
        retrieved = playLaunchChannelEntityMgr.findById(playLaunchChannel2.getId());
        Assert.assertNull(retrieved);
    }

    @AfterClass(groups = "functional")
    public void teardown() throws Exception {
        if (playLaunchChannel1 != null && playLaunchChannel1.getId() != null) {
            playLaunchChannelEntityMgr.deleteByChannelId(playLaunchChannel1.getId(), true);
        }
        if (playLaunchChannel2 != null && playLaunchChannel2.getId() != null) {
            playLaunchChannelEntityMgr.deleteByChannelId(playLaunchChannel2.getId(), true);
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
        playLaunchChannel.setId(NamingUtils.randomSuffix("pl", 16));
        return playLaunchChannel;
    }

}
