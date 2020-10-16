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
import com.latticeengines.domain.exposed.cdl.DataIntegrationEventType;
import com.latticeengines.domain.exposed.cdl.DataIntegrationStatusMonitor;
import com.latticeengines.domain.exposed.cdl.DataIntegrationStatusMonitorMessage;
import com.latticeengines.domain.exposed.pls.LaunchState;
import com.latticeengines.domain.exposed.pls.LookupIdMap;
import com.latticeengines.domain.exposed.pls.Play;
import com.latticeengines.domain.exposed.pls.PlayLaunch;
import com.latticeengines.domain.exposed.pls.PlayLaunchChannel;

public class FailedWorkflowStatusHandlerTestNG extends StatusHandlerTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(FailedWorkflowStatusHandlerTestNG.class);

    @Inject
    private DataIntegrationStatusMonitoringEntityMgr dataIntegrationStatusMonitoringEntityMgr;

    @Inject
    private PlayLaunchChannelService playLaunchChannelService;

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
        cleanupPlayLaunches();

        play = createPlay();
        lookupIdMap = createLookupIdMap();
        channel = createPlayLaunchChannel(play, lookupIdMap);
        playLaunch = createPlayLaunch(play, channel);
        statusMessage = createStatusMessage(playLaunch, DataIntegrationEventType.Failed);
        statusMonitor = createStatusMonitor(statusMessage);
    }

    @Test(groups = "functional")
    public void testFailedWorkflowStatusHandler() {
        Assert.assertEquals(channel.getCurrentLaunchedAccountUniverseTable(), CURRENT_TABLE);
        Assert.assertEquals(channel.getCurrentLaunchedContactUniverseTable(), CURRENT_TABLE);
        Assert.assertNotEquals(playLaunch.getLaunchState(), LaunchState.SyncFailed);

        FailedWorkflowStatusHandler handler = new FailedWorkflowStatusHandler(playLaunchService,
                                                                              playLaunchChannelService,
                                                                              dataIntegrationStatusMonitoringEntityMgr);
        handler.handleWorkflowState(statusMonitor, statusMessage);

        String launchId = playLaunch.getLaunchId();
        playLaunch = playLaunchService.findByLaunchId(launchId, false);
        channel = playLaunchService.findPlayLaunchChannelByLaunchId(launchId);

        Assert.assertEquals(statusMonitor.getStatus(), DataIntegrationEventType.Failed.toString());
        Assert.assertEquals(playLaunch.getLaunchState(), LaunchState.SyncFailed);
        Assert.assertEquals(channel.getCurrentLaunchedAccountUniverseTable(), PREVIOUS_TABLE);
        Assert.assertEquals(channel.getCurrentLaunchedContactUniverseTable(), PREVIOUS_TABLE);
        Assert.assertEquals(channel.getPreviousLaunchedAccountUniverseTable(), PREVIOUS_TABLE);
        Assert.assertEquals(channel.getPreviousLaunchedContactUniverseTable(), PREVIOUS_TABLE);
        teardown(launchId, channel.getId(), play.getName());
    }
}
