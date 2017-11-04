package com.latticeengines.pls.entitymanager;

import java.util.Date;
import java.util.List;

import com.latticeengines.db.exposed.entitymgr.BaseEntityMgr;
import com.latticeengines.domain.exposed.pls.LaunchState;
import com.latticeengines.domain.exposed.pls.Play;
import com.latticeengines.domain.exposed.pls.PlayLaunch;
import com.latticeengines.domain.exposed.pls.PlayLaunchDashboard.LaunchSummary;
import com.latticeengines.domain.exposed.pls.PlayLaunchDashboard.Stats;

public interface PlayLaunchEntityMgr extends BaseEntityMgr<PlayLaunch> {

    void create(PlayLaunch entity);

    PlayLaunch findByLaunchId(String launchId);

    void deleteByLaunchId(String launchId);

    PlayLaunch findByPlayAndTimestamp(Long playId, Date timestamp);

    List<PlayLaunch> findByPlayId(Long playId, List<LaunchState> states);

    PlayLaunch findLatestByPlayId(Long playId, List<LaunchState> states);

    List<PlayLaunch> findByState(LaunchState state);

    List<LaunchSummary> findDashboardEntries(Long playId, List<LaunchState> states, Long startTimestamp, Long offset,
            Long max, String sortby, boolean descending, Long endTimestamp);

    Long findDashboardEntriesCount(Long playId, List<LaunchState> states, Long startTimestamp, Long endTimestamp);

    Stats findDashboardCumulativeStats(Long playId, List<LaunchState> states, Long startTimestamp, Long endTimestamp);

    List<Play> findDashboardPlaysWithLaunches(Long playId, List<LaunchState> launchStates, Long startTimestamp,
            Long endTimestamp);
}
