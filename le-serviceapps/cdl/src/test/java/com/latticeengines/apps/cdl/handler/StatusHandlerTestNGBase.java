package com.latticeengines.apps.cdl.handler;

import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.apps.cdl.entitymgr.DataIntegrationStatusMessageEntityMgr;
import com.latticeengines.apps.cdl.entitymgr.DataIntegrationStatusMonitoringEntityMgr;
import com.latticeengines.apps.cdl.entitymgr.LookupIdMappingEntityMgr;
import com.latticeengines.apps.cdl.service.DataIntegrationStatusMonitoringService;
import com.latticeengines.apps.cdl.service.PlayLaunchChannelService;
import com.latticeengines.apps.cdl.service.PlayLaunchService;
import com.latticeengines.apps.cdl.service.PlayService;
import com.latticeengines.apps.cdl.service.PlayTypeService;
import com.latticeengines.apps.cdl.service.SegmentService;
import com.latticeengines.apps.cdl.testframework.CDLFunctionalTestNGBase;
import com.latticeengines.common.exposed.util.NamingUtils;
import com.latticeengines.domain.exposed.cdl.CDLExternalSystemName;
import com.latticeengines.domain.exposed.cdl.CDLExternalSystemType;
import com.latticeengines.domain.exposed.cdl.DataIntegrationEventType;
import com.latticeengines.domain.exposed.cdl.DataIntegrationStatusMonitor;
import com.latticeengines.domain.exposed.cdl.DataIntegrationStatusMonitorMessage;
import com.latticeengines.domain.exposed.cdl.ExternalIntegrationWorkflowType;
import com.latticeengines.domain.exposed.cdl.LaunchType;
import com.latticeengines.domain.exposed.cdl.MessageType;
import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.domain.exposed.pls.LaunchState;
import com.latticeengines.domain.exposed.pls.LookupIdMap;
import com.latticeengines.domain.exposed.pls.Play;
import com.latticeengines.domain.exposed.pls.PlayLaunch;
import com.latticeengines.domain.exposed.pls.PlayLaunchChannel;
import com.latticeengines.domain.exposed.pls.PlayType;
import com.latticeengines.domain.exposed.pls.RatingBucketName;
import com.latticeengines.domain.exposed.pls.cdl.channel.MarketoChannelConfig;
import com.latticeengines.metadata.entitymgr.TableEntityMgr;

public class StatusHandlerTestNGBase extends CDLFunctionalTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(StatusHandlerTestNGBase.class);

    protected static long CURRENT_TIME_MILLIS = System.currentTimeMillis();

    @Inject
    private DataIntegrationStatusMonitoringService dataIntegrationStatusMonitoringService;

    @Inject
    private DataIntegrationStatusMessageEntityMgr dataIntegrationStatusMessageEntityMgr;

    @Inject
    private DataIntegrationStatusMonitoringEntityMgr dataIntegrationStatusMonitoringEntityMgr;

    @Inject
    private LookupIdMappingEntityMgr lookupIdMappingEntityMgr;

    @Inject
    protected SegmentService segmentService;

    @Inject
    private PlayLaunchChannelService playLaunchChannelService;

    @Inject
    private PlayLaunchService playLaunchService;

    @Inject
    private PlayService playService;

    @Inject
    protected TableEntityMgr tableEntityMgr;

    @Inject
    private PlayTypeService playTypeService;

    protected String NAME = "play" + CURRENT_TIME_MILLIS;
    protected String ORG_ID = "Org_" + CURRENT_TIME_MILLIS;
    protected String DISPLAY_NAME = "play Harder";
    protected String PLAY_TARGET_SEGMENT_NAME = "Play Target Segment - 2";
    protected String CREATED_BY = "lattice@lattice-engines.com";

    protected String CURRENT_ACCOUNT_TABLE = "Current_account_table";
    protected String CURRENT_CONTACT_TABLE = "Current_contact_table";
    protected String PREVIOUS_ACCOUNT_TABLE = "Previous_account_table";
    protected String PREVIOUS_CONTACT_TABLE = "Previous_contact_table";

    protected Play createPlay() {
        Play play = new Play();
        List<PlayType> allPlayTypes = playTypeService.getAllPlayTypes(mainCustomerSpace);
        List<MetadataSegment> allSegments = segmentService.getSegments();
        Date timestamp = new Date(CURRENT_TIME_MILLIS);

        play.setName(NAME);
        play.setDisplayName(DISPLAY_NAME);
        play.setTenant(mainTestTenant);
        play.setCreated(timestamp);
        play.setUpdated(timestamp);
        play.setCreatedBy(CREATED_BY);
        play.setUpdatedBy(CREATED_BY);
        play.setTargetSegment(allSegments.get(0));
        play.setPlayType(allPlayTypes.get(0));

        return playService.createOrUpdate(play, mainTestTenant.getId());
    }

    protected LookupIdMap createLookupIdMap() {
        String orgName = "TEST_Org_" + System.currentTimeMillis();
        String orgId = "Org_" + System.currentTimeMillis();
        LookupIdMap lookupIdMap = new LookupIdMap();
        lookupIdMap.setExternalSystemType(CDLExternalSystemType.MAP);
        lookupIdMap.setExternalSystemName(CDLExternalSystemName.Marketo);
        lookupIdMap.setOrgId(orgId);
        lookupIdMap.setOrgName(orgName);
        lookupIdMap.setIsRegistered(true);

        log.info("Created LookupIdMap for " + lookupIdMap.getExternalSystemName().toString());

        return lookupIdMappingEntityMgr.createExternalSystem(lookupIdMap);
    }

    protected void createTables() {
        createTable(CURRENT_ACCOUNT_TABLE);
        createTable(CURRENT_CONTACT_TABLE);
        createTable(PREVIOUS_ACCOUNT_TABLE);
        createTable(PREVIOUS_CONTACT_TABLE);
    }

    protected PlayLaunchChannel createPlayLaunchChannel(Play play, LookupIdMap lookupIdMap) {
        createTables();

        PlayLaunchChannel channel = new PlayLaunchChannel();
        channel.setTenant(mainTestTenant);
        channel.setPlay(play);
        channel.setLookupIdMap(lookupIdMap);
        channel.setCreatedBy(CREATED_BY);
        channel.setUpdatedBy(CREATED_BY);
        channel.setLaunchType(LaunchType.DELTA);
        channel.setId(NamingUtils.randomSuffix("Launch__", 16));
        channel.setLaunchUnscored(false);
        channel.setChannelConfig(new MarketoChannelConfig());
        channel.setCurrentLaunchedAccountUniverseTable(CURRENT_ACCOUNT_TABLE);
        channel.setCurrentLaunchedContactUniverseTable(CURRENT_CONTACT_TABLE);
        channel.setPreviousLaunchedAccountUniverseTable(PREVIOUS_ACCOUNT_TABLE);
        channel.setPreviousLaunchedContactUniverseTable(PREVIOUS_CONTACT_TABLE);

        playLaunchChannelService.create(NAME, channel);
        log.info("Created PlayLaunchChannel with ID: " + channel.getId());

        return channel;
    }

    protected PlayLaunch createPlayLaunch(Play play, PlayLaunchChannel channel) {
        PlayLaunch playLaunch = new PlayLaunch();
        Set<RatingBucketName> bucketsToLaunch = new TreeSet<>(Arrays.asList(RatingBucketName.values()));

        playLaunch.setLaunchId(NamingUtils.randomSuffix("pl", 16));
        playLaunch.setTenant(mainTestTenant);
        playLaunch.setLaunchState(LaunchState.Launching);
        playLaunch.setPlay(play);
        playLaunch.setBucketsToLaunch(bucketsToLaunch);
        playLaunch.setDestinationAccountId("Marketo_ACC1");
        playLaunch.setDestinationOrgId(ORG_ID);
        playLaunch.setDestinationSysType(CDLExternalSystemType.MAP);
        playLaunch.setDestinationSysName(CDLExternalSystemName.Marketo);
        playLaunch.setLaunchType(LaunchType.DELTA);
        playLaunch.setPlayLaunchChannel(channel);
        playLaunch.setChannelConfig(new MarketoChannelConfig());
        playLaunch.setCreatedBy(CREATED_BY);
        playLaunch.setUpdatedBy(CREATED_BY);

        playLaunchService.create(playLaunch);
        log.info("Created PlayLaunch with launch ID: " + playLaunch.getLaunchId());

        return playLaunch;
    }

    protected DataIntegrationStatusMonitorMessage createStatusMessage(PlayLaunch playLaunch,
                                                                      DataIntegrationEventType eventType) {
        String WORKFLOW_REQ_ID = UUID.randomUUID().toString();
        DataIntegrationStatusMonitorMessage statusMessage = new DataIntegrationStatusMonitorMessage();
        statusMessage.setTenantName(mainTestTenant.getName());
        statusMessage.setMessageType(MessageType.Event.toString());
        statusMessage.setEventType(eventType.toString());
        statusMessage.setWorkflowRequestId(WORKFLOW_REQ_ID);
        statusMessage.setEntityId(playLaunch.getLaunchId());
        statusMessage.setEntityName("PlayLaunch");
        statusMessage.setExternalSystemId(UUID.randomUUID().toString());
        statusMessage.setOperation(ExternalIntegrationWorkflowType.EXPORT.toString());
        statusMessage.setEventTime(new Date());

        log.info("Created DataIntegrationStatusMonitorMessage");

        return statusMessage;
    }

    protected DataIntegrationStatusMonitor createStatusMonitor(DataIntegrationStatusMonitorMessage statusMessage) {
        DataIntegrationStatusMonitor statusMonitor = new DataIntegrationStatusMonitor(statusMessage, mainTestTenant);
        dataIntegrationStatusMonitoringEntityMgr.createStatus(statusMonitor);

        log.info("Created DataIntegrationStatusMonitor");

        return statusMonitor;
    }

    protected void cleanupPlayLaunches() {
        for (PlayLaunch launch : playLaunchService.findByState(LaunchState.Launching)) {
            playLaunchService.deleteByLaunchId(launch.getLaunchId(), false);
        }
    }

    protected void teardown(String launchId, String channelId, String playName) {
        if (launchId != null) {
            playLaunchService.deleteByLaunchId(launchId, false);
        }

        if (channelId != null) {
            playLaunchChannelService.deleteByChannelId(channelId, false);
        }

        if (playName != null) {
            playService.deleteByName(playName, false);
        }

        teardownTables();
    }

    private void teardownTables() {
        if (tableEntityMgr.findByName(CURRENT_ACCOUNT_TABLE) != null) {
            tableEntityMgr.deleteByName(CURRENT_ACCOUNT_TABLE);
        }

        if (tableEntityMgr.findByName(CURRENT_CONTACT_TABLE) != null) {
            tableEntityMgr.deleteByName(CURRENT_CONTACT_TABLE);
        }

        if (tableEntityMgr.findByName(PREVIOUS_ACCOUNT_TABLE) != null) {
            tableEntityMgr.deleteByName(PREVIOUS_ACCOUNT_TABLE);
        }

        if (tableEntityMgr.findByName(PREVIOUS_CONTACT_TABLE) != null) {
            tableEntityMgr.deleteByName(PREVIOUS_CONTACT_TABLE);
        }
    }
}
