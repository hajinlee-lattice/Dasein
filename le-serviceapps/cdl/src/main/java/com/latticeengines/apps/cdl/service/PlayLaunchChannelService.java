package com.latticeengines.apps.cdl.service;

import java.util.List;

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

    PlayLaunchChannel findChannelAndPlayById(String channelId);

    PlayLaunchChannel findById(String channelId, boolean useWriterRepo);

    PlayLaunch createNewLaunchByPlayAndChannel(Play play, PlayLaunchChannel playLaunchChannel, PlayLaunch launch,
                                               boolean autoLaunch);

    PlayLaunchChannel updateAudience(String audienceId, String audienceName, String playLaunchId);

    void updateAttributeSetNameToDefault(String attributeSetName);

    void updatePreviousLaunchedAccountUniverseWithCurrent(PlayLaunchChannel playLaunchChannel);

    void updatePreviousLaunchedContactUniverseWithCurrent(PlayLaunchChannel playLaunchChannel);

    PlayLaunchChannel recoverLaunchUniverse(PlayLaunchChannel channel);
}
