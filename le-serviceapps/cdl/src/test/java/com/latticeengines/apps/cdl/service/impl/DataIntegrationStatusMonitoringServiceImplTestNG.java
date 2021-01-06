package com.latticeengines.apps.cdl.service.impl;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.apps.cdl.entitymgr.DataIntegrationStatusMessageEntityMgr;
import com.latticeengines.apps.cdl.entitymgr.LookupIdMappingEntityMgr;
import com.latticeengines.apps.cdl.entitymgr.PlayEntityMgr;
import com.latticeengines.apps.cdl.service.DataIntegrationStatusMonitoringService;
import com.latticeengines.apps.cdl.service.PlayLaunchChannelService;
import com.latticeengines.apps.cdl.service.PlayLaunchService;
import com.latticeengines.apps.cdl.service.PlayTypeService;
import com.latticeengines.apps.cdl.testframework.CDLFunctionalTestNGBase;
import com.latticeengines.common.exposed.util.NamingUtils;
import com.latticeengines.common.exposed.util.SleepUtils;
import com.latticeengines.domain.exposed.cdl.AccountEventDetail;
import com.latticeengines.domain.exposed.cdl.AudienceEventDetail;
import com.latticeengines.domain.exposed.cdl.CDLExternalSystemName;
import com.latticeengines.domain.exposed.cdl.CDLExternalSystemType;
import com.latticeengines.domain.exposed.cdl.DataIntegrationEventType;
import com.latticeengines.domain.exposed.cdl.DataIntegrationStatusMessage;
import com.latticeengines.domain.exposed.cdl.DataIntegrationStatusMonitor;
import com.latticeengines.domain.exposed.cdl.DataIntegrationStatusMonitorMessage;
import com.latticeengines.domain.exposed.cdl.ExternalIntegrationWorkflowType;
import com.latticeengines.domain.exposed.cdl.InitiatedEventDetail;
import com.latticeengines.domain.exposed.cdl.LaunchType;
import com.latticeengines.domain.exposed.cdl.MessageType;
import com.latticeengines.domain.exposed.cdl.ProgressEventDetail;
import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.domain.exposed.pls.LaunchState;
import com.latticeengines.domain.exposed.pls.LookupIdMap;
import com.latticeengines.domain.exposed.pls.Play;
import com.latticeengines.domain.exposed.pls.PlayLaunch;
import com.latticeengines.domain.exposed.pls.PlayLaunchChannel;
import com.latticeengines.domain.exposed.pls.PlayType;
import com.latticeengines.domain.exposed.pls.RatingBucketName;
import com.latticeengines.domain.exposed.pls.cdl.channel.MarketoChannelConfig;

public class DataIntegrationStatusMonitoringServiceImplTestNG extends CDLFunctionalTestNGBase {

    private static long CURRENT_TIME_MILLIS = System.currentTimeMillis();

    private static final Logger log = LoggerFactory.getLogger(DataIntegrationStatusMonitoringServiceImplTestNG.class);

    @Inject
    private DataIntegrationStatusMonitoringService dataIntegrationStatusMonitoringService;

    @Inject
    private DataIntegrationStatusMessageEntityMgr dataIntegrationStatusMessageEntityMgr;

    private Long BATCH_ID = 1110L;
    private String ENTITY_NAME = "PlayLaunch";
    private String SOURCE_FILE = "dropfolder/tenant/atlas/Data/Files/Exports/MAP/Marketo/example.csv";
    private String NAME = "play" + CURRENT_TIME_MILLIS;
    private String DISPLAY_NAME = "play Harder";
    private String PLAY_TARGET_SEGMENT_NAME = "Play Target Segment";
    private String CREATED_BY = "lattice@lattice-engines.com";
    private String CRON_EXPRESSION = "0 0 12 ? * WED *";

    private Play play;
    private PlayLaunch playLaunch1;
    private PlayLaunch playLaunch2;
    private PlayLaunch playLaunchWithOrg1;

    private String org1 = "org1_" + CURRENT_TIME_MILLIS;
    private String org2 = "org2_" + CURRENT_TIME_MILLIS;
    private String changedOrgId = "changedOrgId_" + CURRENT_TIME_MILLIS;
    private String orgName = "org_name";
    private List<PlayType> playTypes;
    private Set<RatingBucketName> bucketsToLaunch;
    private MetadataSegment playTargetSegment;

    @Inject
    private PlayEntityMgr playEntityMgr;

    @Inject
    private PlayLaunchService playLaunchService;

    @Inject
    private PlayLaunchChannelService playLaunchChannelService;

    @Inject
    private PlayTypeService playTypeService;

    @Inject
    private LookupIdMappingEntityMgr lookupIdMappingEntityMgr;

    @BeforeClass(groups = "functional")
    public void setup() throws Exception {
        setupTestEnvironmentWithDummySegment();
        cleanupPlayLaunches();

        Date timestamp = new Date(System.currentTimeMillis());

        playTypes = playTypeService.getAllPlayTypes(mainCustomerSpace);
        playTargetSegment = createMetadataSegment(PLAY_TARGET_SEGMENT_NAME);
        assertNotNull(playTargetSegment);
        assertEquals(playTargetSegment.getDisplayName(), PLAY_TARGET_SEGMENT_NAME);

        play = new Play();
        play.setName(NAME);
        play.setDisplayName(DISPLAY_NAME);
        play.setTenant(mainTestTenant);
        play.setCreated(timestamp);
        play.setUpdated(timestamp);
        play.setCreatedBy(CREATED_BY);
        play.setUpdatedBy(CREATED_BY);
        play.setTargetSegment(playTargetSegment);
        play.setPlayType(playTypes.get(0));

        playEntityMgr.create(play);
        play = playEntityMgr.getPlayByName(NAME, false);
        assertNotNull(play.getTargetSegment());
        assertEquals(play.getTargetSegment().getDisplayName(), PLAY_TARGET_SEGMENT_NAME);

        LookupIdMap lookupIdMapMarketo = createLookIdMap(org1, orgName,
                CDLExternalSystemType.MAP,
                CDLExternalSystemName.Marketo);

        bucketsToLaunch = new TreeSet<>(Arrays.asList(RatingBucketName.values()));

        playLaunch1 = createPlayLaunch(play, NamingUtils.randomSuffix("pl", 16), LaunchState.Launching, bucketsToLaunch,
                "SFDC_ACC1", org1, CDLExternalSystemType.CRM, CDLExternalSystemName.Salesforce, CREATED_BY, CREATED_BY,
                lookupIdMapMarketo);
        playLaunchService.create(playLaunch1);

        playLaunch2 = createPlayLaunch(play, NamingUtils.randomSuffix("pl", 16), LaunchState.Launching, bucketsToLaunch,
                "SFDC_ACC2", org2, CDLExternalSystemType.CRM, CDLExternalSystemName.Salesforce, CREATED_BY, CREATED_BY,
                lookupIdMapMarketo);
        playLaunchService.create(playLaunch2);

        playLaunchWithOrg1 = createPlayLaunch(play, NamingUtils.randomSuffix("pl", 16), LaunchState.Launching,
                bucketsToLaunch,
                "SFDC_ACC3", org1, CDLExternalSystemType.CRM, CDLExternalSystemName.Salesforce, CREATED_BY, CREATED_BY,
                lookupIdMapMarketo);
        playLaunchService.create(playLaunchWithOrg1);
    }

    private void cleanupPlayLaunches() {
        for (PlayLaunch launch : playLaunchService.findByState(LaunchState.Launching)) {
            playLaunchService.deleteByLaunchId(launch.getLaunchId(), false);
        }
    }

    @Test(groups = "functional")
    public void testCreateAndGet() {
        String workflowRequestId = UUID.randomUUID().toString();
        DataIntegrationStatusMonitorMessage statusMessage = createDefaultStatusMessage(workflowRequestId,
                DataIntegrationEventType.WorkflowSubmitted.toString(), playLaunch1.getId());
        dataIntegrationStatusMonitoringService.createOrUpdateStatuses(generateListMessages(statusMessage));

        DataIntegrationStatusMonitor statusMonitor = findDataIntegrationMonitorByWorkflowReqId(workflowRequestId);

        Assert.assertNotNull(statusMonitor);
        Assert.assertEquals(DataIntegrationEventType.WorkflowSubmitted.toString(), statusMonitor.getStatus());
        Assert.assertNotNull(statusMonitor.getEventSubmittedTime());
    }

    private DataIntegrationStatusMonitorMessage createDefaultStatusMessage(String workflowRequestId, String eventType,
            String entityId) {
        DataIntegrationStatusMonitorMessage statusMessage = new DataIntegrationStatusMonitorMessage();
        statusMessage.setTenantName(mainTestTenant.getName());
        statusMessage.setWorkflowRequestId(workflowRequestId);
        statusMessage.setEntityId(entityId);
        statusMessage.setEntityName(ENTITY_NAME);
        statusMessage.setExternalSystemId("org_" + CURRENT_TIME_MILLIS);
        statusMessage.setOperation(ExternalIntegrationWorkflowType.EXPORT.toString());
        statusMessage.setMessageType(MessageType.Event.toString());
        statusMessage.setMessage("This workflow has been submitted");
        statusMessage.setEventType(eventType);
        statusMessage.setEventTime(new Date());
        statusMessage.setSourceFile(SOURCE_FILE);
        statusMessage.setEventDetail(null);
        return statusMessage;
    }

    private DataIntegrationStatusMonitor findDataIntegrationMonitorByWorkflowReqId(String workflowRequestId) {
        addReaderDelay();
        return dataIntegrationStatusMonitoringService
                .getStatus(workflowRequestId);
    }

    @Test(groups = "functional")
    public void testCreateWithIncorrectOrder() {
        String workflowRequestId = UUID.randomUUID().toString();
        DataIntegrationStatusMonitorMessage statusMessage = new DataIntegrationStatusMonitorMessage();
        statusMessage.setWorkflowRequestId(workflowRequestId);
        statusMessage.setEventType(DataIntegrationEventType.Initiated.toString());
        statusMessage.setEventTime(new Date());
        statusMessage.setMessageType(MessageType.Event.toString());
        statusMessage.setMessage("test");

        boolean exceptionThrown = false;
        try {
            dataIntegrationStatusMonitoringService.createOrUpdateStatuses(generateListMessages(statusMessage));
        } catch (Exception e) {
            log.info("Caught exception creating status monitor: " + e.getMessage());
            exceptionThrown = true;
        }

        Assert.assertTrue(exceptionThrown);
    }

    @Test(groups = "functional")
    public void testUpdateWithCorrectOrder() {
        String workflowRequestId = UUID.randomUUID().toString();
        DataIntegrationStatusMonitorMessage createStatusMonitorMessage = createDefaultStatusMessage(workflowRequestId,
                DataIntegrationEventType.WorkflowSubmitted.toString(), playLaunch1.getId());

        dataIntegrationStatusMonitoringService.createOrUpdateStatuses(generateListMessages(createStatusMonitorMessage));
        DataIntegrationStatusMonitor statusMonitor = findDataIntegrationMonitorByWorkflowReqId(workflowRequestId);

        Assert.assertNotNull(statusMonitor);

        DataIntegrationStatusMonitorMessage updateStatusMonitorMessage = new DataIntegrationStatusMonitorMessage();
        updateStatusMonitorMessage.setWorkflowRequestId(workflowRequestId);
        updateStatusMonitorMessage.setEventType(DataIntegrationEventType.ExportStart.toString());
        updateStatusMonitorMessage.setEventTime(new Date());
        updateStatusMonitorMessage.setMessageType(MessageType.Event.toString());
        updateStatusMonitorMessage.setMessage("test");
        updateStatusMonitorMessage.setEventDetail(null);

        dataIntegrationStatusMonitoringService.createOrUpdateStatuses(generateListMessages(updateStatusMonitorMessage));
        statusMonitor = findDataIntegrationMonitorByWorkflowReqId(workflowRequestId);

        Assert.assertNotNull(statusMonitor);
        Assert.assertNotNull(statusMonitor.getTenant());
        Assert.assertNotNull(statusMonitor.getEventStartedTime());
        Assert.assertNotNull(statusMonitor.getEventSubmittedTime());
        Assert.assertEquals(DataIntegrationEventType.ExportStart.toString(), statusMonitor.getStatus());

        PlayLaunch playLaunch = playLaunchService.findByLaunchId(playLaunch1.getId(), false);
        Assert.assertEquals(LaunchState.Syncing, playLaunch.getLaunchState());

        testCompletedMessage(workflowRequestId, playLaunch1);

        testAudienceSizeUpdateMessage(workflowRequestId, playLaunch1);

        testDestinationAccountCreationMessage(workflowRequestId, playLaunch1);
        List<PlayLaunch> playLaunches = playLaunchService.findByDestinationOrgId(changedOrgId);
        Assert.assertEquals(playLaunches.size(), 2);

        List<DataIntegrationStatusMessage> messages = dataIntegrationStatusMessageEntityMgr
                .getAllStatusMessages(statusMonitor.getPid());

        Assert.assertNotNull(messages);
        Assert.assertEquals(messages.size(), 5);
    }

    @Test(groups = "functional")
    public void testUpdateWithIncorrectOrder() {
        String workflowRequestId = UUID.randomUUID().toString();
        DataIntegrationStatusMonitorMessage createStatusMonitorMessage = createDefaultStatusMessage(workflowRequestId,
                DataIntegrationEventType.WorkflowSubmitted.toString(), playLaunch2.getId());

        dataIntegrationStatusMonitoringService.createOrUpdateStatuses(generateListMessages(createStatusMonitorMessage));
        DataIntegrationStatusMonitor statusMonitor = findDataIntegrationMonitorByWorkflowReqId(workflowRequestId);

        Assert.assertNotNull(statusMonitor);

        testCompletedMessage(workflowRequestId, playLaunch2);

        testAudienceSizeUpdateMessage(workflowRequestId, playLaunch2);

        DataIntegrationStatusMonitorMessage updateStatusMonitorMessage = new DataIntegrationStatusMonitorMessage();
        updateStatusMonitorMessage.setWorkflowRequestId(workflowRequestId);
        updateStatusMonitorMessage.setEventType(DataIntegrationEventType.Initiated.toString());
        updateStatusMonitorMessage.setEventTime(new Date());
        updateStatusMonitorMessage.setMessageType(MessageType.Event.toString());
        updateStatusMonitorMessage.setEventDetail(new InitiatedEventDetail());

        dataIntegrationStatusMonitoringService.createOrUpdateStatuses(generateListMessages(updateStatusMonitorMessage));
        statusMonitor = findDataIntegrationMonitorByWorkflowReqId(workflowRequestId);

        Assert.assertNotNull(statusMonitor);
        Assert.assertNotNull(statusMonitor.getEventSubmittedTime());
        Assert.assertNotNull(statusMonitor.getEventCompletedTime());
        Assert.assertEquals(DataIntegrationEventType.AudienceSizeUpdate.toString(), statusMonitor.getStatus());

        List<DataIntegrationStatusMessage> messages = dataIntegrationStatusMessageEntityMgr
                .getAllStatusMessages(statusMonitor.getPid());
        Assert.assertNotNull(messages);
        Assert.assertEquals(messages.size(), 4);

        // throw new Exception();
    }

    private void addReaderDelay() {
        SleepUtils.sleep(2000L);
    }

    private List<DataIntegrationStatusMonitorMessage> generateListMessages(
            DataIntegrationStatusMonitorMessage... statusMessage) {
        return Arrays.asList(statusMessage);
    }

    private void testCompletedMessage(String workflowRequestId, PlayLaunch testPlayLaunch) {
        DataIntegrationStatusMonitorMessage updateStatusMonitorMessage = new DataIntegrationStatusMonitorMessage();
        updateStatusMonitorMessage.setWorkflowRequestId(workflowRequestId);
        updateStatusMonitorMessage.setEventType(DataIntegrationEventType.Completed.toString());
        updateStatusMonitorMessage.setEventTime(new Date());
        updateStatusMonitorMessage.setMessageType(MessageType.Event.toString());

        ProgressEventDetail eventDetail = new ProgressEventDetail();
        eventDetail.setFailed(1L);
        eventDetail.setProcessed(2L);
        eventDetail.setTotalRecordsSubmitted(4L);
        eventDetail.setDuplicates(1L);
        updateStatusMonitorMessage.setEventDetail(eventDetail);

        dataIntegrationStatusMonitoringService.createOrUpdateStatuses(generateListMessages(updateStatusMonitorMessage));
        DataIntegrationStatusMonitor statusMonitor = findDataIntegrationMonitorByWorkflowReqId(workflowRequestId);

        Assert.assertNotNull(statusMonitor);
        Assert.assertNotNull(statusMonitor.getEventSubmittedTime());
        Assert.assertNotNull(statusMonitor.getEventCompletedTime());
        Assert.assertEquals(DataIntegrationEventType.Completed.toString(), statusMonitor.getStatus());

        PlayLaunch playLaunch = playLaunchService.findByLaunchId(testPlayLaunch.getId(), false);
        Assert.assertEquals(LaunchState.PartialSync, playLaunch.getLaunchState());
        Assert.assertEquals(Long.valueOf(1), playLaunch.getContactsErrored());
        Assert.assertEquals(Long.valueOf(1), playLaunch.getContactsDuplicated());
    }

    private void testAudienceSizeUpdateMessage(String workflowRequestId, PlayLaunch testPlayLaunch) {
        DataIntegrationStatusMonitorMessage updateStatusMonitorMessage = new DataIntegrationStatusMonitorMessage();
        updateStatusMonitorMessage.setWorkflowRequestId(workflowRequestId);
        updateStatusMonitorMessage.setEventType(DataIntegrationEventType.AudienceSizeUpdate.toString());
        updateStatusMonitorMessage.setEventTime(new Date());
        updateStatusMonitorMessage.setMessageType(MessageType.Event.toString());

        AudienceEventDetail audienceDetail = new AudienceEventDetail();
        audienceDetail.setAudienceSize(2L);
        audienceDetail.setMatchedCount(2L);
        updateStatusMonitorMessage.setEventDetail(audienceDetail);

        dataIntegrationStatusMonitoringService.createOrUpdateStatuses(generateListMessages(updateStatusMonitorMessage));
        DataIntegrationStatusMonitor statusMonitor = findDataIntegrationMonitorByWorkflowReqId(workflowRequestId);

        Assert.assertNotNull(statusMonitor);
        Assert.assertEquals(DataIntegrationEventType.AudienceSizeUpdate.toString(), statusMonitor.getStatus());

        PlayLaunch playLaunch = playLaunchService.findByLaunchId(testPlayLaunch.getId(), false);
        Assert.assertEquals(Long.valueOf(2), playLaunch.getAudienceSize());
        Assert.assertEquals(Long.valueOf(2), playLaunch.getMatchedCount());
    }

    private void testDestinationAccountCreationMessage(String workflowRequestId, PlayLaunch testPlayLaunch) {
        DataIntegrationStatusMonitorMessage updateStatusMonitorMessage = new DataIntegrationStatusMonitorMessage();
        updateStatusMonitorMessage.setWorkflowRequestId(workflowRequestId);
        updateStatusMonitorMessage.setEventType(DataIntegrationEventType.DestinationAccountCreation.toString());
        updateStatusMonitorMessage.setEventTime(new Date());
        updateStatusMonitorMessage.setMessageType(MessageType.Event.toString());

        AccountEventDetail accountDetail = new AccountEventDetail();
        accountDetail.setAccountId(changedOrgId);
        updateStatusMonitorMessage.setEventDetail(accountDetail);

        dataIntegrationStatusMonitoringService.createOrUpdateStatuses(generateListMessages(updateStatusMonitorMessage));

        PlayLaunch playLaunch = playLaunchService.findByLaunchId(testPlayLaunch.getId(), true);
        Assert.assertEquals(playLaunch.getDestinationOrgId(), changedOrgId);
        Assert.assertEquals(playLaunch.getPlayLaunchChannel().getLookupIdMap().getOrgId(), changedOrgId);
    }

    private PlayLaunch createPlayLaunch(Play play, String launchId, LaunchState launchState, Set<RatingBucketName> bucketToLaunch,
                                        String destinationAccountId, String destOrgId, CDLExternalSystemType destSysType,
            CDLExternalSystemName cdlExternalSystemName, String createdBy, String updatedBy, LookupIdMap lookupIdMap) {
        PlayLaunchChannel playLaunchChannel = createPlayLaunchChannel(play, lookupIdMap);
        playLaunchChannel.setIsAlwaysOn(false);
        playLaunchChannel.setChannelConfig(new MarketoChannelConfig());
        playLaunchChannel = playLaunchChannelService.create(play.getName(), playLaunchChannel);

        PlayLaunch playLaunch = new PlayLaunch();
        playLaunch.setLaunchId(launchId);
        playLaunch.setTenant(mainTestTenant);
        playLaunch.setLaunchState(launchState);
        playLaunch.setPlay(play);
        playLaunch.setBucketsToLaunch(bucketToLaunch);
        playLaunch.setDestinationAccountId(destinationAccountId);
        playLaunch.setDestinationOrgId(destOrgId);
        playLaunch.setDestinationSysType(destSysType);
        playLaunch.setDestinationSysName(cdlExternalSystemName);
        playLaunch.setLaunchType(LaunchType.FULL);
        playLaunch.setCreatedBy(updatedBy);
        playLaunch.setUpdatedBy(createdBy);
        playLaunch.setPlayLaunchChannel(playLaunchChannel);
        return playLaunch;
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
        playLaunchChannel.setLaunchUnscored(false);
        playLaunchChannel.setCronScheduleExpression(CRON_EXPRESSION);
        return playLaunchChannel;
    }

    private LookupIdMap createLookIdMap(String orgId, String orgName, CDLExternalSystemType type,
            CDLExternalSystemName name) {
        LookupIdMap lookupIdMap = new LookupIdMap();
        lookupIdMap.setExternalSystemType(type);
        lookupIdMap.setExternalSystemName(name);
        lookupIdMap.setOrgId(orgId);
        lookupIdMap.setOrgName(orgName);
        lookupIdMap = lookupIdMappingEntityMgr.createExternalSystem(lookupIdMap);
        return lookupIdMap;
    }

    @Test(groups = "functional")
    public void testGetAllStatusesByEntityNameAndIds() {
        List<DataIntegrationStatusMonitor> dataIntegrationStatusMonitors = dataIntegrationStatusMonitoringService
                .getAllStatusesByEntityNameAndIds(mainTestTenant.getId(), ENTITY_NAME,
                        Collections.singletonList(playLaunch1.getId()));
        assertNotNull(dataIntegrationStatusMonitors);
        assertEquals(dataIntegrationStatusMonitors.size(), 1);
        DataIntegrationStatusMonitor statusMonitor = dataIntegrationStatusMonitors.get(0);
        assertNotNull(statusMonitor);
        assertEquals(statusMonitor.getEntityId(), playLaunch1.getId());
    }
}
