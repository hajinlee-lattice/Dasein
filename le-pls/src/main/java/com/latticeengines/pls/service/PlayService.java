package com.latticeengines.pls.service;

import java.util.List;

import com.latticeengines.domain.exposed.pls.LaunchState;
import com.latticeengines.domain.exposed.pls.Play;
import com.latticeengines.domain.exposed.pls.PlayLaunch;
import com.latticeengines.domain.exposed.pls.PlayLaunchChannel;
import com.latticeengines.domain.exposed.pls.PlayLaunchDashboard;

public interface PlayService {

    List<Play> getPlays(Boolean shouldLoadCoverage, String ratingEngineId);

    Play getPlay(String playName);

    Play createOrUpdate(Play play);

    void delete(String playName, Boolean hardDelete);

    List<PlayLaunchChannel> getPlayLaunchChannels(String playName, Boolean includeUnlaunchedChannels);

    PlayLaunchChannel createPlayLaunchChannel(String playName, PlayLaunchChannel playLaunchChannel, Boolean launchNow);

    PlayLaunchChannel updatePlayLaunchChannel(String playName, String channelId, PlayLaunchChannel playLaunchChannel, Boolean launchNow);

    PlayLaunchDashboard getPlayLaunchDashboard(String playName, String orgId, String externalSysType, List<LaunchState> launchStates,
                                               Long startTimestamp, Long offset, Long max, String sortBy, boolean descending, Long endTimestamp);

    Long getPlayLaunchDashboardEntriesCount(String playName, String orgId,
                                            String externalSysType, List<LaunchState> launchStates, Long startTimestamp, Long endTimestamp);

    PlayLaunch createPlayLaunch(String playName, PlayLaunch playLaunch);

    PlayLaunch updatePlayLaunch(String playName, String launchId, PlayLaunch playLaunch);

    PlayLaunch launchPlay(String playName, String launchId);

    List<PlayLaunch> getPlayLaunches(String playName, List<LaunchState> launchStates);

    PlayLaunch getPlayLaunch(String playName, String launchId);

    PlayLaunch updatePlayLaunch(String playName, String launchId, LaunchState action);

    void deletePlayLaunch(String playName, String launchId, Boolean hardDelete);

}
