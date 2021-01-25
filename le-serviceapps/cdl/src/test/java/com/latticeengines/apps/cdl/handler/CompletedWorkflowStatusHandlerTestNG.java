package com.latticeengines.apps.cdl.handler;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.apps.cdl.entitymgr.DataIntegrationStatusMonitoringEntityMgr;
import com.latticeengines.apps.cdl.service.PlayLaunchChannelService;
import com.latticeengines.apps.cdl.service.PlayLaunchService;
import com.latticeengines.apps.cdl.service.PlayService;
import com.latticeengines.domain.exposed.cdl.DataIntegrationEventType;
import com.latticeengines.domain.exposed.cdl.DataIntegrationStatusMonitor;
import com.latticeengines.domain.exposed.cdl.DataIntegrationStatusMonitorMessage;
import com.latticeengines.domain.exposed.cdl.ProgressEventDetail;
import com.latticeengines.domain.exposed.pls.LaunchState;
import com.latticeengines.domain.exposed.pls.LookupIdMap;
import com.latticeengines.domain.exposed.pls.Play;
import com.latticeengines.domain.exposed.pls.PlayLaunch;
import com.latticeengines.domain.exposed.pls.PlayLaunchChannel;

public class CompletedWorkflowStatusHandlerTestNG extends StatusHandlerTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(CompletedWorkflowStatusHandlerTestNG.class);

    @Inject
    private DataIntegrationStatusMonitoringEntityMgr dataIntegrationStatusMonitoringEntityMgr;

    @Inject
    private PlayLaunchChannelService playLaunchChannelService;

    @Inject
    private PlayLaunchService playLaunchService;

    @Inject
    private PlayService playService;

    private Play play;
    private PlayLaunch playLaunch;
    private LookupIdMap lookupIdMap;
    private DataIntegrationStatusMonitorMessage statusMessage;
    private DataIntegrationStatusMonitor statusMonitor;
    private PlayLaunchChannel channel;

    @BeforeClass(groups = "functional")
    public void setup() throws Exception {
        setupTestEnvironmentWithDummySegment();
    }

    @Test(groups = "functional")
    public void testCompletedWorkflowStatusSynced() {
        createAll();
        Assert.assertEquals(channel.getPreviousLaunchedAccountUniverseTable(), PREVIOUS_ACCOUNT_TABLE);
        Assert.assertEquals(channel.getPreviousLaunchedContactUniverseTable(), PREVIOUS_CONTACT_TABLE);

        Long totalRecords = 100L;
        Long processed = 100L;
        Long failed = 0L;
        Long duplicates = 0L;
        createEventDetail(statusMessage, totalRecords, processed, failed, duplicates);

        CompletedWorkflowStatusHandler handler = new CompletedWorkflowStatusHandler(playLaunchService,
                                                                                    playLaunchChannelService,
                                                                                    dataIntegrationStatusMonitoringEntityMgr);
        handler.handleWorkflowState(statusMonitor, statusMessage);

        String launchId = playLaunch.getLaunchId();
        playLaunch = playLaunchService.findByLaunchId(launchId, false);
        channel = playLaunchService.findPlayLaunchChannelByLaunchId(launchId);

        Assert.assertEquals(statusMonitor.getStatus(), DataIntegrationEventType.Completed.toString());
        Assert.assertEquals(playLaunch.getLaunchState(), LaunchState.Synced);
        Assert.assertEquals(channel.getPreviousLaunchedAccountUniverseTable(), CURRENT_ACCOUNT_TABLE);
        Assert.assertEquals(channel.getPreviousLaunchedContactUniverseTable(), CURRENT_CONTACT_TABLE);
        Assert.assertEquals(channel.getCurrentLaunchedAccountUniverseTable(), CURRENT_ACCOUNT_TABLE);
        Assert.assertEquals(channel.getCurrentLaunchedContactUniverseTable(), CURRENT_CONTACT_TABLE);
        teardown(launchId, channel.getId(), play.getName());
    }

    @Test(groups = "functional")
    public void testCompletedWorkflowStatusPartialSync() {
        createAll();
        Assert.assertEquals(channel.getPreviousLaunchedAccountUniverseTable(), PREVIOUS_ACCOUNT_TABLE);
        Assert.assertEquals(channel.getPreviousLaunchedContactUniverseTable(), PREVIOUS_CONTACT_TABLE);

        Long totalRecords = 100L;
        Long processed = 90L;
        Long failed = 10L;
        Long duplicates = 0L;
        createEventDetail(statusMessage, totalRecords, processed, failed, duplicates);

        CompletedWorkflowStatusHandler handler = new CompletedWorkflowStatusHandler(playLaunchService,
                                                                                    playLaunchChannelService,
                                                                                    dataIntegrationStatusMonitoringEntityMgr);
        handler.handleWorkflowState(statusMonitor, statusMessage);

        String launchId = playLaunch.getLaunchId();
        playLaunch = playLaunchService.findByLaunchId(launchId, false);
        channel = playLaunchService.findPlayLaunchChannelByLaunchId(launchId);

        Assert.assertEquals(statusMonitor.getStatus(), DataIntegrationEventType.Completed.toString());
        Assert.assertEquals(playLaunch.getLaunchState(), LaunchState.PartialSync);
        Assert.assertEquals(channel.getPreviousLaunchedAccountUniverseTable(), CURRENT_ACCOUNT_TABLE);
        Assert.assertEquals(channel.getPreviousLaunchedContactUniverseTable(), CURRENT_CONTACT_TABLE);
        Assert.assertEquals(channel.getCurrentLaunchedAccountUniverseTable(), CURRENT_ACCOUNT_TABLE);
        Assert.assertEquals(channel.getCurrentLaunchedContactUniverseTable(), CURRENT_CONTACT_TABLE);
        teardown(launchId, channel.getId(), play.getName());
    }

    @Test(groups = "functional")
    public void testCompletedWorkflowStatusSyncFailed() {
        createAll();
        Assert.assertEquals(channel.getCurrentLaunchedAccountUniverseTable(), CURRENT_ACCOUNT_TABLE);
        Assert.assertEquals(channel.getCurrentLaunchedContactUniverseTable(), CURRENT_CONTACT_TABLE);

        Long totalRecords = 100L;
        Long processed = 0L;
        Long failed = 90L;
        Long duplicates = 10L;
        createEventDetail(statusMessage, totalRecords, processed, failed, duplicates);

        CompletedWorkflowStatusHandler handler = new CompletedWorkflowStatusHandler(playLaunchService,
                                                                                    playLaunchChannelService,
                                                                                    dataIntegrationStatusMonitoringEntityMgr);
        handler.handleWorkflowState(statusMonitor, statusMessage);

        String launchId = playLaunch.getLaunchId();
        playLaunch = playLaunchService.findByLaunchId(launchId, false);
        channel = playLaunchService.findPlayLaunchChannelByLaunchId(launchId);

        Assert.assertEquals(statusMonitor.getStatus(), DataIntegrationEventType.Completed.toString());
        Assert.assertEquals(playLaunch.getLaunchState(), LaunchState.SyncFailed);
        Assert.assertEquals(channel.getPreviousLaunchedAccountUniverseTable(), PREVIOUS_ACCOUNT_TABLE);
        Assert.assertEquals(channel.getPreviousLaunchedContactUniverseTable(), PREVIOUS_CONTACT_TABLE);
        Assert.assertEquals(channel.getCurrentLaunchedAccountUniverseTable(), PREVIOUS_ACCOUNT_TABLE);
        Assert.assertEquals(channel.getCurrentLaunchedContactUniverseTable(), PREVIOUS_CONTACT_TABLE);
        teardown(launchId, channel.getId(), play.getName());
    }

    @Test(groups = "functional")
    public void testCompletedWorkflowStatusSyncedWithNoProcessed() {
        createAll();
        Assert.assertEquals(channel.getPreviousLaunchedAccountUniverseTable(), PREVIOUS_ACCOUNT_TABLE);
        Assert.assertEquals(channel.getPreviousLaunchedContactUniverseTable(), PREVIOUS_CONTACT_TABLE);

        Long totalRecords = 0L;
        Long processed = 0L;
        Long failed = 0L;
        Long duplicates = 0L;
        createEventDetail(statusMessage, totalRecords, processed, failed, duplicates);

        CompletedWorkflowStatusHandler handler = new CompletedWorkflowStatusHandler(playLaunchService,
                playLaunchChannelService, dataIntegrationStatusMonitoringEntityMgr);
        handler.handleWorkflowState(statusMonitor, statusMessage);

        handler.handleWorkflowState(statusMonitor, statusMessage);

        String launchId = playLaunch.getLaunchId();
        playLaunch = playLaunchService.findByLaunchId(launchId, false);
        channel = playLaunchService.findPlayLaunchChannelByLaunchId(launchId);

        Assert.assertEquals(statusMonitor.getStatus(), DataIntegrationEventType.Completed.toString());
        Assert.assertEquals(playLaunch.getLaunchState(), LaunchState.Synced);
        Assert.assertEquals(channel.getPreviousLaunchedAccountUniverseTable(), CURRENT_ACCOUNT_TABLE);
        Assert.assertEquals(channel.getPreviousLaunchedContactUniverseTable(), CURRENT_CONTACT_TABLE);
        Assert.assertEquals(channel.getCurrentLaunchedAccountUniverseTable(), CURRENT_ACCOUNT_TABLE);
        Assert.assertEquals(channel.getCurrentLaunchedContactUniverseTable(), CURRENT_CONTACT_TABLE);
        teardown(launchId, channel.getId(), play.getName());
    }

    private void createAll() {
        cleanupPlayLaunches();

        play = createPlay();
        lookupIdMap = createLookupIdMap();
        channel = createPlayLaunchChannel(play, lookupIdMap);
        playLaunch = createPlayLaunch(play, channel);
        statusMessage = createStatusMessage(playLaunch, DataIntegrationEventType.Completed);
        statusMonitor = createStatusMonitor(statusMessage);
    }

    private void createEventDetail(DataIntegrationStatusMonitorMessage statusMessage,
                                   Long totalRecords, Long processed, Long failed, Long duplicates) {
        ProgressEventDetail eventDetail = new ProgressEventDetail();
        eventDetail.setTotalRecordsSubmitted(totalRecords);
        eventDetail.setProcessed(processed);
        eventDetail.setFailed(failed);
        eventDetail.setDuplicates(duplicates);
        statusMessage.setEventDetail(eventDetail);
        log.info("Created eventDetail");
    }
}
