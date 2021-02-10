package com.latticeengines.apps.cdl.handler;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.apps.cdl.service.PlayLaunchService;
import com.latticeengines.domain.exposed.cdl.AcxiomReceived;
import com.latticeengines.domain.exposed.cdl.DataIntegrationEventType;
import com.latticeengines.domain.exposed.cdl.DataIntegrationStatusMonitor;
import com.latticeengines.domain.exposed.cdl.DataIntegrationStatusMonitorMessage;
import com.latticeengines.domain.exposed.pls.LookupIdMap;
import com.latticeengines.domain.exposed.pls.Play;
import com.latticeengines.domain.exposed.pls.PlayLaunch;
import com.latticeengines.domain.exposed.pls.PlayLaunchChannel;

public class ReceivedFromAcxiomWorkflowStatusHandlerTestNG extends StatusHandlerTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(ReceivedFromAcxiomWorkflowStatusHandlerTestNG.class);

    @Inject
    private PlayLaunchService playLaunchService;

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

        Long receivedCount = 10000L;
        createEventDetail(statusMessage, receivedCount);

        ReceivedFromAcxiomWorkflowStatusHandler handler = new ReceivedFromAcxiomWorkflowStatusHandler(
                playLaunchService);
        handler.handleWorkflowState(statusMonitor, statusMessage);

        String launchId = playLaunch.getLaunchId();
        playLaunch = playLaunchService.findByLaunchId(launchId, false);

        Assert.assertNotNull(playLaunch.getRecordsStats());
        Assert.assertEquals(playLaunch.getRecordsStats().getRecordsReceivedFromAcxiom(), receivedCount);
        teardown(launchId, channel.getId(), play.getName());
    }

    private void createAll() {
        cleanupPlayLaunches();

        play = createPlay();
        lookupIdMap = createLookupIdMap();
        channel = createPlayLaunchChannel(play, lookupIdMap);
        playLaunch = createPlayLaunch(play, channel);
        statusMessage = createStatusMessage(playLaunch, DataIntegrationEventType.ReceivedFromAcxiom);
        statusMonitor = createStatusMonitor(statusMessage);
    }

    private void createEventDetail(DataIntegrationStatusMonitorMessage statusMessage, Long receivedCount) {
        AcxiomReceived eventDetail = new AcxiomReceived();
        eventDetail.setReceivedCount(receivedCount);
        statusMessage.setEventDetail(eventDetail);
        log.info("Created eventDetail");
    }
}
