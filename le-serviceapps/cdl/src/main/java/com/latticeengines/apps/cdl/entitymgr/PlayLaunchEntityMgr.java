package com.latticeengines.apps.cdl.entitymgr;

import java.util.Date;
import java.util.List;

import org.apache.commons.lang3.tuple.Pair;

import com.latticeengines.db.exposed.entitymgr.BaseEntityMgr;
import com.latticeengines.domain.exposed.pls.LaunchState;
import com.latticeengines.domain.exposed.pls.Play;
import com.latticeengines.domain.exposed.pls.PlayLaunch;
import com.latticeengines.domain.exposed.pls.PlayLaunchDashboard.LaunchSummary;
import com.latticeengines.domain.exposed.pls.PlayLaunchDashboard.Stats;

public interface PlayLaunchEntityMgr extends BaseEntityMgr<PlayLaunch> {

    void create(PlayLaunch entity);

    PlayLaunch findByLaunchId(String launchId);

    void deleteByLaunchId(String launchId, boolean hardDelete);

    PlayLaunch findByPlayAndTimestamp(Long playId, Date timestamp);

    List<PlayLaunch> findByPlayId(Long playId, List<LaunchState> states);

    PlayLaunch findLatestByPlayId(Long playId, List<LaunchState> states);

    List<PlayLaunch> findByState(LaunchState state);

    List<LaunchSummary> findDashboardEntries(Long playId, List<LaunchState> states, Long startTimestamp, Long offset,
            Long max, String sortby, boolean descending, Long endTimestamp, String orgId, String externalSysType);

    Long findDashboardEntriesCount(Long playId, List<LaunchState> states, Long startTimestamp, Long endTimestamp,
            String orgId, String externalSysType);

    Stats findDashboardCumulativeStats(Long playId, List<LaunchState> states, Long startTimestamp, Long endTimestamp,
            String orgId, String externalSysType);

    List<Play> findDashboardPlaysWithLaunches(Long playId, List<LaunchState> launchStates, Long startTimestamp,
            Long endTimestamp, String orgId, String externalSysType);

    List<Pair<String, String>> findDashboardOrgIdWithLaunches(Long playId, List<LaunchState> launchStates,
            Long startTimestamp, Long endTimestamp, String orgId, String externalSysType);
}
