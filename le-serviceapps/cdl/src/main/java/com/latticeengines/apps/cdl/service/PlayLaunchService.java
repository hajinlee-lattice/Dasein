package com.latticeengines.apps.cdl.service;

import java.util.Date;
import java.util.List;

import com.latticeengines.domain.exposed.pls.LaunchState;
import com.latticeengines.domain.exposed.pls.PlayLaunch;
import com.latticeengines.domain.exposed.pls.PlayLaunchChannel;
import com.latticeengines.domain.exposed.pls.PlayLaunchDashboard;

public interface PlayLaunchService {

    void create(PlayLaunch playLaunch);

    PlayLaunch findByLaunchId(String launchId, boolean inflate);

    PlayLaunchChannel findPlayLaunchChannelByLaunchId(String launchId);

    void deleteByLaunchId(String launchId, boolean hardDelete);

    PlayLaunch findByPlayAndTimestamp(Long playId, Date timestamp);

    List<PlayLaunch> findByPlayId(Long playId, List<LaunchState> launchStates);

    PlayLaunch findLatestByPlayId(Long playId, List<LaunchState> launchStates);

    PlayLaunch findLatestByPlayAndSysOrg(Long playId, String orgId);

    PlayLaunch findLatestByChannel(Long playLaunchChannelId);

    PlayLaunch findLatestTerminalLaunchByChannel(Long playLaunchChannelId);

    List<PlayLaunch> findByState(LaunchState state);

    List<PlayLaunch> findByStateAcrossTenants(LaunchState state, Long max);

    PlayLaunch update(PlayLaunch existingPlayLaunch);

    PlayLaunchDashboard getDashboard(Long playId, List<LaunchState> launchStates, Long startTimestamp, Long offset,
            Long max, String sortby, boolean descending, Long endTimestamp, String orgId, String externalSysType,
            boolean skipLoadingAllLookupIdMapping);

    Long getDashboardEntriesCount(Long playId, List<LaunchState> launchStates, Long startTimestamp, Long endTimestamp,
            String orgId, String externalSysType);

    PlayLaunch updateAudience(String audienceId, String audienceName, String playLaunchId);
}
