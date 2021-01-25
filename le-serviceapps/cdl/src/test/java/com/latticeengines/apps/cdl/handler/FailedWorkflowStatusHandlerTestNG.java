package com.latticeengines.apps.cdl.handler;

import java.util.Collections;

import javax.inject.Inject;

import org.springframework.retry.support.RetryTemplate;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.apps.cdl.entitymgr.DataIntegrationStatusMonitoringEntityMgr;
import com.latticeengines.apps.cdl.service.PlayLaunchChannelService;
import com.latticeengines.apps.cdl.service.PlayLaunchService;
import com.latticeengines.common.exposed.util.RetryUtils;
import com.latticeengines.domain.exposed.cdl.DataIntegrationEventType;
import com.latticeengines.domain.exposed.cdl.DataIntegrationStatusMonitor;
import com.latticeengines.domain.exposed.cdl.DataIntegrationStatusMonitorMessage;
import com.latticeengines.domain.exposed.pls.LaunchState;
import com.latticeengines.domain.exposed.pls.LookupIdMap;
import com.latticeengines.domain.exposed.pls.Play;
import com.latticeengines.domain.exposed.pls.PlayLaunch;
import com.latticeengines.domain.exposed.pls.PlayLaunchChannel;

public class FailedWorkflowStatusHandlerTestNG extends StatusHandlerTestNGBase {

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
        playLaunch.setContactsLaunched(100L);
        playLaunch = playLaunchService.update(playLaunch);

        statusMessage = createStatusMessage(playLaunch, DataIntegrationEventType.Failed);
        statusMonitor = createStatusMonitor(statusMessage);
    }

    @Test(groups = "functional")
    public void testFailedWorkflowStatusHandler() {
        Assert.assertEquals(channel.getCurrentLaunchedAccountUniverseTable(), CURRENT_ACCOUNT_TABLE);
        Assert.assertEquals(channel.getCurrentLaunchedContactUniverseTable(), CURRENT_CONTACT_TABLE);
        Assert.assertNotEquals(playLaunch.getLaunchState(), LaunchState.SyncFailed);

        FailedWorkflowStatusHandler handler = new FailedWorkflowStatusHandler(playLaunchService,
                                                                              playLaunchChannelService,
                                                                              dataIntegrationStatusMonitoringEntityMgr);
        handler.handleWorkflowState(statusMonitor, statusMessage);

        String launchId = playLaunch.getLaunchId();
        RetryTemplate retry = RetryUtils.getRetryTemplate(5, //
                Collections.singleton(AssertionError.class), null);

        retry.execute(context -> {
            PlayLaunch updatedPlayLaunch = playLaunchService.findByLaunchId(launchId, false);
            PlayLaunchChannel updatedChannel = playLaunchService.findPlayLaunchChannelByLaunchId(launchId);

            Assert.assertEquals(statusMonitor.getStatus(), DataIntegrationEventType.Failed.toString());
            Assert.assertEquals(updatedPlayLaunch.getLaunchState(), LaunchState.SyncFailed);
            Assert.assertEquals(updatedPlayLaunch.getContactsErrored(), Long.valueOf(100));
            Assert.assertEquals(updatedChannel.getPreviousLaunchedAccountUniverseTable(), PREVIOUS_ACCOUNT_TABLE);
            Assert.assertEquals(updatedChannel.getPreviousLaunchedContactUniverseTable(), PREVIOUS_CONTACT_TABLE);
            Assert.assertEquals(updatedChannel.getCurrentLaunchedAccountUniverseTable(), PREVIOUS_ACCOUNT_TABLE);
            Assert.assertEquals(updatedChannel.getCurrentLaunchedContactUniverseTable(), PREVIOUS_CONTACT_TABLE);
            return true;
        });

        teardown(launchId, channel.getId(), play.getName());
    }
}
