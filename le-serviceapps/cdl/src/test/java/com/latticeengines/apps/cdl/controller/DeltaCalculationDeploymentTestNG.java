package com.latticeengines.apps.cdl.controller;

import static com.latticeengines.domain.exposed.cdl.CDLExternalSystemName.AWS_S3;
import static com.latticeengines.domain.exposed.cdl.CDLExternalSystemName.Marketo;
import static com.latticeengines.domain.exposed.cdl.CDLExternalSystemName.Salesforce;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.apps.cdl.entitymgr.PlayLaunchChannelEntityMgr;
import com.latticeengines.apps.cdl.testframework.CDLDeploymentTestNGBase;
import com.latticeengines.domain.exposed.admin.LatticeFeatureFlag;
import com.latticeengines.domain.exposed.cdl.CDLExternalSystemType;
import com.latticeengines.domain.exposed.cdl.LaunchType;
import com.latticeengines.domain.exposed.pls.PlayLaunchChannel;
import com.latticeengines.domain.exposed.pls.RatingEngineStatus;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.proxy.exposed.cdl.PlayProxy;
import com.latticeengines.proxy.exposed.cdl.RatingEngineProxy;
import com.latticeengines.security.exposed.service.TenantService;
import com.latticeengines.testframework.exposed.domain.TestPlayChannelConfig;
import com.latticeengines.testframework.exposed.domain.TestPlaySetupConfig;
import com.latticeengines.testframework.exposed.service.CDLTestDataService;
import com.latticeengines.testframework.service.impl.TestPlayCreationHelper;

public class DeltaCalculationDeploymentTestNG extends CDLDeploymentTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(DeltaCalculationDeploymentTestNG.class);

    @Inject
    private CDLTestDataService cdlTestDataService;

    @Inject
    private PlayProxy playProxy;

    @Inject
    private RatingEngineProxy ratingEngineProxy;

    @Inject
    private PlayLaunchChannelEntityMgr playLaunchChannelEntityMgr;

    @Inject
    private TestPlayCreationHelper playCreationHelper1;

    @Inject
    private TestPlayCreationHelper playCreationHelper2;

    @Inject
    private TestPlayCreationHelper playCreationHelper3;

    @Inject
    private TenantService tenantService;

    private List<TestPlaySetupConfig> testPlaySetupConfigs = new ArrayList<>();

    @BeforeClass(groups = "deployment-app", enabled = false)
    public void setup() throws Exception {

        List<String> tenants = Arrays.asList("LETest1565029942183.LETest1565029942183.Production",
                "JLM_1565035479103.JLM_1565035479103.Production", "JLM_1565035901844.JLM_1565035901844.Production",
                "JLM_1565036306867.JLM_1565036306867.Production", "JLM_1565036665852.JLM_1565036665852.Production",
                "JLM_1565037001764.JLM_1565037001764.Production", "JLM_1565037252727.JLM_1565037252727.Production",
                "JLM_1565038905021.JLM_1565038905021.Production");

        tenants.forEach(t -> {
            Tenant tenant = tenantService.findByTenantId(t);
            if (tenant != null) {
                tenantService.discardTenant(tenant);
            }
        });

        Map<String, Boolean> featureFlags = new HashMap<>();
        featureFlags.put(LatticeFeatureFlag.ENABLE_EXTERNAL_INTEGRATION.getName(), true);
        featureFlags.put(LatticeFeatureFlag.ALPHA_FEATURE.getName(), true);
        featureFlags.put(LatticeFeatureFlag.ALWAYS_ON_CAMPAIGNS.getName(), true);

        TestPlaySetupConfig testPlaySetupConfig1 = new TestPlaySetupConfig.Builder().existingTenant("JLM_1564644778424")
                .addChannel(new TestPlayChannelConfig.Builder().destinationSystemType(CDLExternalSystemType.CRM)
                        .destinationSystemName(Salesforce).destinationSystemId("Channel_" + System.currentTimeMillis())
                        .isAlwaysOn(true).cronSchedule("0 0/10 * 1/1 * ? *").launchType(LaunchType.FULL).build())
                .addChannel(new TestPlayChannelConfig.Builder().destinationSystemType(CDLExternalSystemType.MAP)
                        .destinationSystemName(Marketo).destinationSystemId("Channel_" + System.currentTimeMillis())
                        .trayAuthenticationId(UUID.randomUUID().toString()).audienceId(UUID.randomUUID().toString())
                        .build())
                .addChannel(new TestPlayChannelConfig.Builder().destinationSystemType(CDLExternalSystemType.FILE_SYSTEM)
                        .destinationSystemName(AWS_S3).destinationSystemId("Channel_" + System.currentTimeMillis())
                        .isAlwaysOn(true).cronSchedule("0 0/10 * 1/1 * ? *").launchType(LaunchType.FULL).build())
                .featureFlags(featureFlags).build();
        playCreationHelper1.setupTenantAndCreatePlay(testPlaySetupConfig1);
        log.info(
                "Tenant 1: " + playCreationHelper1.getTenant().getId() + " Play: " + playCreationHelper1.getPlayName());
        playProxy.deletePlay(playCreationHelper1.getTenant().getId(), playCreationHelper1.getPlayName(), false);

        TestPlaySetupConfig testPlaySetupConfig2 = new TestPlaySetupConfig.Builder().existingTenant("JLM_1564644930638")
                .addChannel(new TestPlayChannelConfig.Builder().destinationSystemType(CDLExternalSystemType.CRM)
                        .destinationSystemName(Salesforce).destinationSystemId("Channel_" + System.currentTimeMillis())
                        .isAlwaysOn(true).cronSchedule("0 0/10 * 1/1 * ? *").launchType(LaunchType.FULL).build())
                .addChannel(new TestPlayChannelConfig.Builder().destinationSystemType(CDLExternalSystemType.MAP)
                        .destinationSystemName(Marketo).destinationSystemId("Channel_" + System.currentTimeMillis())
                        .trayAuthenticationId(UUID.randomUUID().toString()).audienceId(UUID.randomUUID().toString())
                        .build())
                .addChannel(new TestPlayChannelConfig.Builder().destinationSystemType(CDLExternalSystemType.FILE_SYSTEM)
                        .destinationSystemName(AWS_S3).destinationSystemId("Channel_" + System.currentTimeMillis())
                        .isAlwaysOn(true).cronSchedule("0 0/10 * 1/1 * ? *").launchType(LaunchType.FULL).build())
                .featureFlags(featureFlags).build();
        playCreationHelper2.setupTenantAndCreatePlay(testPlaySetupConfig2);
        playCreationHelper2.getRatingEngine().setStatus(RatingEngineStatus.INACTIVE);

        ratingEngineProxy.createOrUpdateRatingEngine(playCreationHelper2.getTenant().getId(),
                playCreationHelper2.getRatingEngine());
        log.info(
                "Tenant 2: " + playCreationHelper2.getTenant().getId() + " Play: " + playCreationHelper2.getPlayName());

        TestPlaySetupConfig testPlaySetupConfig3 = new TestPlaySetupConfig.Builder().existingTenant("JLM_1564645091401")
                .addChannel(new TestPlayChannelConfig.Builder().destinationSystemType(CDLExternalSystemType.CRM)
                        .destinationSystemName(Salesforce).destinationSystemId("Channel_" + System.currentTimeMillis())
                        .isAlwaysOn(true).cronSchedule("0 0/10 * 1/1 * ? *").launchType(LaunchType.FULL).build())
                .addChannel(new TestPlayChannelConfig.Builder().destinationSystemType(CDLExternalSystemType.MAP)
                        .destinationSystemName(Marketo).destinationSystemId("Channel_" + System.currentTimeMillis())
                        .trayAuthenticationId(UUID.randomUUID().toString()).audienceId(UUID.randomUUID().toString())
                        .build())
                .addChannel(new TestPlayChannelConfig.Builder().destinationSystemType(CDLExternalSystemType.FILE_SYSTEM)
                        .destinationSystemName(AWS_S3).destinationSystemId("Channel_" + System.currentTimeMillis())
                        .isAlwaysOn(true).cronSchedule("0 0/10 * 1/1 * ? *").launchType(LaunchType.FULL).build())
                .featureFlags(featureFlags).build();
        playCreationHelper3.setupTenantAndCreatePlay(testPlaySetupConfig3);
        log.info(
                "Tenant 3: " + playCreationHelper3.getTenant().getId() + " Play: " + playCreationHelper3.getPlayName());
    }

    @Test(groups = "deployment-app", enabled = false)
    public void testGettingScheduledChannels() {
        List<PlayLaunchChannel> channels = playLaunchChannelEntityMgr.getAllValidScheduledChannels();
        Assert.assertEquals(channels.size(), 9);
    }

    // @Test(groups = "deployment-app", enabled = false)
    // public void testDeltaCalculation() {
    // PlayLaunchChannel testChannel = playLaunchChannelEntityMgr.findByPlayName(playCreationHelper1.getPlayName())
    // .get(0);
    // String appId = playProxy.kickoffDeltaCalculation(playCreationHelper1.getCustomerSpace(),
    // playCreationHelper1.getPlayName(), testChannel.getId());
    // log.info("AppID: " + appId);
    // log.info("Queued a delta calculation job for campaignId " + playCreationHelper1.getPlayName() + ", ChannelID: "
    // + testChannel.getId() + " : " + appId);
    //
    // }
}
