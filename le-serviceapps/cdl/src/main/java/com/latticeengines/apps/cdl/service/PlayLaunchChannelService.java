package com.latticeengines.apps.cdl.service;

import java.util.List;

import com.latticeengines.domain.exposed.pls.PlayLaunchChannel;

public interface PlayLaunchChannelService {

    PlayLaunchChannel create(PlayLaunchChannel playLaunchChannel);

    PlayLaunchChannel update(PlayLaunchChannel playLaunchChannel);

    List<PlayLaunchChannel> findByIsAlwaysOnTrue();

    List<PlayLaunchChannel> getPlayLaunchChannels(String playName, Boolean includeUnlaunchedChannels);

    PlayLaunchChannel findByPlayNameAndLookupIdMapId(String playName, String lookupId);

    PlayLaunchChannel findById(String channelId);

}
