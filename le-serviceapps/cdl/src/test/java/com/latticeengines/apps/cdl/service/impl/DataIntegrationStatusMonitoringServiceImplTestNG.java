package com.latticeengines.apps.cdl.service.impl;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;

import javax.inject.Inject;

import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.apps.cdl.entitymgr.DataIntegrationStatusMessageEntityMgr;
import com.latticeengines.apps.cdl.entitymgr.PlayEntityMgr;
import com.latticeengines.apps.cdl.service.DataIntegrationStatusMonitoringService;
import com.latticeengines.apps.cdl.service.PlayLaunchService;
import com.latticeengines.apps.cdl.service.PlayTypeService;
import com.latticeengines.apps.cdl.testframework.CDLFunctionalTestNGBase;
import com.latticeengines.common.exposed.util.NamingUtils;
import com.latticeengines.domain.exposed.cdl.CDLExternalSystemType;
import com.latticeengines.domain.exposed.cdl.DataIntegrationEventType;
import com.latticeengines.domain.exposed.cdl.DataIntegrationStatusMessage;
import com.latticeengines.domain.exposed.cdl.DataIntegrationStatusMonitor;
import com.latticeengines.domain.exposed.cdl.DataIntegrationStatusMonitorMessage;
import com.latticeengines.domain.exposed.cdl.ExternalIntegrationWorkflowType;
import com.latticeengines.domain.exposed.cdl.InitiatedEventDetail;
import com.latticeengines.domain.exposed.cdl.MessageType;
import com.latticeengines.domain.exposed.cdl.ProgressEventDetail;
import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.domain.exposed.pls.LaunchState;
import com.latticeengines.domain.exposed.pls.Play;
import com.latticeengines.domain.exposed.pls.PlayLaunch;
import com.latticeengines.domain.exposed.pls.PlayType;
import com.latticeengines.domain.exposed.pls.RatingBucketName;


public class DataIntegrationStatusMonitoringServiceImplTestNG extends CDLFunctionalTestNGBase {

    private static long CURRENT_TIME_MILLIS = System.currentTimeMillis();

    private static final Logger log = LoggerFactory.getLogger(DataIntegrationStatusMonitoringServiceImplTestNG.class);

    @Inject
    DataIntegrationStatusMonitoringService dataIntegrationStatusMonitoringService;

    @Inject
    DataIntegrationStatusMessageEntityMgr dataIntegrationStatusMessageEntityMgr;

    private Long BATCH_ID = 1110L;
    private String ENTITY_NAME = "PlayLaunch";
    private String SOURCE_FILE = "dropfolder/tenant/atlas/Data/Files/Exports/MAP/Marketo/example.csv";
    private String NAME = "play" + CURRENT_TIME_MILLIS;
    private String DISPLAY_NAME = "play Harder";
    private String PLAY_TARGET_SEGMENT_NAME = "Play Target Segment";
    private String CREATED_BY = "lattice@lattice-engines.com";

    private Play play;
    private PlayLaunch playLaunch1;
    private PlayLaunch playLaunch2;

    private String org1 = "org1_" + CURRENT_TIME_MILLIS;
    private String org2 = "org2_" + CURRENT_TIME_MILLIS;
    private List<PlayType> playTypes;
    private Set<RatingBucketName> bucketsToLaunch;
    private MetadataSegment playTargetSegment;

    @Inject
    private PlayEntityMgr playEntityMgr;

    @Inject
    private PlayLaunchService playLaunchService;

    @Inject
    private PlayTypeService playTypeService;

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

        bucketsToLaunch = new TreeSet<>(Arrays.asList(RatingBucketName.values()));

        playLaunch1 = new PlayLaunch();
        playLaunch1.setLaunchId(NamingUtils.randomSuffix("pl", 16));
        playLaunch1.setTenant(mainTestTenant);
        playLaunch1.setLaunchState(LaunchState.Launching);
        playLaunch1.setPlay(play);
        playLaunch1.setBucketsToLaunch(bucketsToLaunch);
        playLaunch1.setDestinationAccountId("SFDC_ACC1");
        playLaunch1.setDestinationOrgId(org1);
        playLaunch1.setDestinationSysType(CDLExternalSystemType.CRM);
        playLaunch1.setCreatedBy(CREATED_BY);
        playLaunch1.setUpdatedBy(CREATED_BY);

        playLaunchService.create(playLaunch1);

        playLaunch2 = new PlayLaunch();
        playLaunch2.setLaunchId(NamingUtils.randomSuffix("pl", 16));
        playLaunch2.setTenant(mainTestTenant);
        playLaunch2.setLaunchState(LaunchState.Launching);
        playLaunch2.setPlay(play);
        playLaunch2.setBucketsToLaunch(bucketsToLaunch);
        playLaunch2.setDestinationAccountId("SFDC_ACC2");
        playLaunch2.setDestinationOrgId(org2);
        playLaunch2.setDestinationSysType(CDLExternalSystemType.CRM);
        playLaunch2.setCreatedBy(CREATED_BY);
        playLaunch2.setUpdatedBy(CREATED_BY);

        playLaunchService.create(playLaunch2);
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
        dataIntegrationStatusMonitoringService.createOrUpdateStatus(statusMessage);

        DataIntegrationStatusMonitor statusMonitor = findDataIntegrationMonitorByWorkflowReqId(workflowRequestId);

        Assert.assertNotNull(statusMonitor);
        Assert.assertEquals(DataIntegrationEventType.WorkflowSubmitted.toString(), statusMonitor.getStatus());
        Assert.assertNotNull(statusMonitor.getEventSubmittedTime());
    }

    private DataIntegrationStatusMonitorMessage createDefaultStatusMessage(String workflowRequestId,
            String eventType, String entityId) {
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
        DataIntegrationStatusMonitor statusMonitor = dataIntegrationStatusMonitoringService
                .getStatus(workflowRequestId);
        return statusMonitor;
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

        Boolean exceptionThrown = false;
        try {
            dataIntegrationStatusMonitoringService.createOrUpdateStatus(statusMessage);
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

        dataIntegrationStatusMonitoringService.createOrUpdateStatus(createStatusMonitorMessage);
        DataIntegrationStatusMonitor statusMonitor = findDataIntegrationMonitorByWorkflowReqId(workflowRequestId);

        Assert.assertNotNull(statusMonitor);

        DataIntegrationStatusMonitorMessage updateStatusMonitorMessage = new DataIntegrationStatusMonitorMessage();
        updateStatusMonitorMessage.setWorkflowRequestId(workflowRequestId);
        updateStatusMonitorMessage.setEventType(DataIntegrationEventType.Initiated.toString());
        updateStatusMonitorMessage.setEventTime(new Date());
        updateStatusMonitorMessage.setMessageType(MessageType.Event.toString());
        updateStatusMonitorMessage.setMessage("test");
        InitiatedEventDetail initiatedEventDetail = new InitiatedEventDetail();
        initiatedEventDetail.setBatchId(BATCH_ID);
        updateStatusMonitorMessage.setEventDetail(initiatedEventDetail);

        dataIntegrationStatusMonitoringService.createOrUpdateStatus(updateStatusMonitorMessage);
        statusMonitor = findDataIntegrationMonitorByWorkflowReqId(workflowRequestId);

        Assert.assertNotNull(statusMonitor);
        Assert.assertNotNull(statusMonitor.getTenant());
        Assert.assertNotNull(statusMonitor.getEventStartedTime());
        Assert.assertNotNull(statusMonitor.getEventSubmittedTime());
        Assert.assertEquals(DataIntegrationEventType.Initiated.toString(), statusMonitor.getStatus());

        PlayLaunch playLaunch = playLaunchService.findByLaunchId(playLaunch1.getId());
        Assert.assertEquals(LaunchState.Syncing, playLaunch.getLaunchState());

        List<DataIntegrationStatusMessage> messages = dataIntegrationStatusMessageEntityMgr
                .getAllStatusMessages(statusMonitor.getPid());

        Assert.assertNotNull(messages);
        Assert.assertEquals(messages.size(), 2);
    }


    @Test(groups = "functional")
    public void testUpdateWithIncorrectOrder() throws Exception {
        String workflowRequestId = UUID.randomUUID().toString();
        DataIntegrationStatusMonitorMessage createStatusMonitorMessage = createDefaultStatusMessage(workflowRequestId,
                DataIntegrationEventType.WorkflowSubmitted.toString(), playLaunch2.getId());

        dataIntegrationStatusMonitoringService.createOrUpdateStatus(createStatusMonitorMessage);
        DataIntegrationStatusMonitor statusMonitor = findDataIntegrationMonitorByWorkflowReqId(workflowRequestId);

        Assert.assertNotNull(statusMonitor);

        DataIntegrationStatusMonitorMessage updateStatusMonitorMessage = new DataIntegrationStatusMonitorMessage();
        updateStatusMonitorMessage.setWorkflowRequestId(workflowRequestId);
        updateStatusMonitorMessage.setEventType(DataIntegrationEventType.Completed.toString());
        updateStatusMonitorMessage.setEventTime(new Date());
        updateStatusMonitorMessage.setMessageType(MessageType.Event.toString());
        ProgressEventDetail eventDetail = new ProgressEventDetail();
        eventDetail.setFailed(1L);
        eventDetail.setProcessed(2L);
        eventDetail.setTotalRecordsSubmitted(4L);
        updateStatusMonitorMessage.setEventDetail(eventDetail);

        dataIntegrationStatusMonitoringService.createOrUpdateStatus(updateStatusMonitorMessage);
        statusMonitor = findDataIntegrationMonitorByWorkflowReqId(workflowRequestId);

        Assert.assertNotNull(statusMonitor);
        Assert.assertNotNull(statusMonitor.getEventSubmittedTime());
        Assert.assertNotNull(statusMonitor.getEventCompletedTime());
        Assert.assertEquals(DataIntegrationEventType.Completed.toString(), statusMonitor.getStatus());

        PlayLaunch playLaunch = playLaunchService.findByLaunchId(playLaunch2.getId());
        Assert.assertEquals(LaunchState.PartialSync, playLaunch.getLaunchState());
        Assert.assertEquals(new Long(1), playLaunch.getContactsErrored());
        Assert.assertEquals(new Long(1), playLaunch.getContactsDuplicated());

        updateStatusMonitorMessage = new DataIntegrationStatusMonitorMessage();
        updateStatusMonitorMessage.setWorkflowRequestId(workflowRequestId);
        updateStatusMonitorMessage.setEventType(DataIntegrationEventType.Initiated.toString());
        updateStatusMonitorMessage.setEventTime(new Date());
        updateStatusMonitorMessage.setMessageType(MessageType.Event.toString());
        updateStatusMonitorMessage.setEventDetail(new InitiatedEventDetail());

        dataIntegrationStatusMonitoringService.createOrUpdateStatus(updateStatusMonitorMessage);
        statusMonitor = findDataIntegrationMonitorByWorkflowReqId(workflowRequestId);

        Assert.assertNotNull(statusMonitor);
        Assert.assertNotNull(statusMonitor.getEventSubmittedTime());
        Assert.assertNotNull(statusMonitor.getEventStartedTime());
        Assert.assertNotNull(statusMonitor.getEventCompletedTime());
        Assert.assertEquals(DataIntegrationEventType.Completed.toString(), statusMonitor.getStatus());

        List<DataIntegrationStatusMessage> messages = dataIntegrationStatusMessageEntityMgr
                .getAllStatusMessages(statusMonitor.getPid());
        Assert.assertNotNull(messages);
        Assert.assertEquals(messages.size(), 3);
        
        // throw new Exception();
    }

    private void addReaderDelay() {
        try {
            Thread.sleep(2000);
        } catch (Exception e) {
            //Ignore
        }
    }

    @Test(groups = "functional")
    public void testGetAllStatusesByEntityNameAndIds() {
        List<DataIntegrationStatusMonitor> dataIntegrationStatusMonitors = dataIntegrationStatusMonitoringService
                .getAllStatusesByEntityNameAndIds(mainTestTenant.getId(), ENTITY_NAME,
                        Arrays.asList(playLaunch1.getId()));
        assertNotNull(dataIntegrationStatusMonitors);
        assertEquals(dataIntegrationStatusMonitors.size(), 1);
        DataIntegrationStatusMonitor statusMonitor = dataIntegrationStatusMonitors.get(0);
        assertNotNull(statusMonitor);
        assertEquals(statusMonitor.getEntityId(), playLaunch1.getId());
    }
}
