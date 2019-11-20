package com.latticeengines.apps.cdl.service;

import java.util.List;

import com.latticeengines.domain.exposed.pls.LaunchState;
import com.latticeengines.domain.exposed.pls.Play;
import com.latticeengines.domain.exposed.pls.PlayLaunch;
import com.latticeengines.domain.exposed.pls.PlayLaunchChannel;

public interface PlayLaunchChannelService {

    PlayLaunchChannel create(String playName, PlayLaunchChannel playLaunchChannel);

    PlayLaunchChannel update(String playName, PlayLaunchChannel playLaunchChannel);

    PlayLaunchChannel updateNextScheduledDate(String playName, String playLaunchChannel);

    PlayLaunchChannel updateLastDeltaWorkflowId(String playName, String playLaunchChannel, Long workflowPid);

    void deleteByChannelId(String channelId, boolean hardDelete);

    List<PlayLaunchChannel> findByIsAlwaysOnTrue();

    List<PlayLaunchChannel> getPlayLaunchChannels(String playName, Boolean includeUnlaunchedChannels);

    PlayLaunchChannel findByPlayNameAndLookupIdMapId(String playName, String lookupId);

    PlayLaunchChannel findById(String channelId);

    PlayLaunch createNewLaunchForChannelByState(Play play, PlayLaunchChannel playLaunchChannel, LaunchState state,
            boolean isAutoLaunch);

    PlayLaunch queueNewLaunchForChannel(Play play, PlayLaunchChannel playLaunchChannel);

    PlayLaunch queueNewLaunchForChannel(Play play, PlayLaunchChannel playLaunchChannel, String addAccountTable,
            String completeContactsTable, String removeAccountsTable, String addContactsTable,
            String removeContactsTable, boolean autoLaunch);

    PlayLaunchChannel updateAudience(String audienceId, String audienceName, PlayLaunch playLaunch);
}
