package com.latticeengines.apps.cdl.service;

import java.util.List;

import com.latticeengines.domain.exposed.pls.Play;
import com.latticeengines.domain.exposed.pls.PlayLaunch;
import com.latticeengines.domain.exposed.pls.PlayLaunchChannel;

public interface PlayLaunchChannelService {

    PlayLaunchChannel create(PlayLaunchChannel playLaunchChannel);

    PlayLaunchChannel update(PlayLaunchChannel playLaunchChannel);

    void deleteByChannelId(String channelId, boolean hardDelete);

    List<PlayLaunchChannel> findByIsAlwaysOnTrue();

    List<PlayLaunchChannel> getPlayLaunchChannels(String playName, Boolean includeUnlaunchedChannels);

    PlayLaunchChannel findByPlayNameAndLookupIdMapId(String playName, String lookupId);

    PlayLaunchChannel findById(String channelId);

    PlayLaunch createPlayLaunchFromChannel(PlayLaunchChannel playLaunchChannel, Play play);

    PlayLaunchChannel updatePlayLaunchChannel(String playName, PlayLaunchChannel playLaunchChannel, Boolean launchNow);

    PlayLaunchChannel createPlayLaunchChannel(String playName, PlayLaunchChannel playLaunchChannel, Boolean launchNow);

}
