package com.latticeengines.apps.cdl.dao;

import java.util.Date;
import java.util.List;

import org.apache.commons.lang3.tuple.Pair;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.domain.exposed.pls.LaunchState;
import com.latticeengines.domain.exposed.pls.LaunchSummary;
import com.latticeengines.domain.exposed.pls.Play;
import com.latticeengines.domain.exposed.pls.PlayLaunch;
import com.latticeengines.domain.exposed.pls.PlayLaunchDashboard.Stats;

public interface PlayLaunchDao extends BaseDao<PlayLaunch> {
    PlayLaunch findByLaunchId(String launchId);

    PlayLaunch findByPlayAndTimestamp(Long playId, Date timestamp);

    List<PlayLaunch> findByPlayId(Long playId, List<LaunchState> states);

    PlayLaunch findLatestByPlayId(Long playId, List<LaunchState> states);

    PlayLaunch findLatestByPlayAndSysOrg(Long playId, String orgId);

    PlayLaunch findLatestByChannel(Long playLaunchChannelId);

    List<PlayLaunch> findByState(LaunchState state);

    List<PlayLaunch> getByStateAcrossTenants(LaunchState state, Long max);

    List<LaunchSummary> findByPlayStatesAndPagination(Long playId, List<LaunchState> states, Long startTimestamp,
            Long offset, Long max, String sortby, boolean descending, Long endTimestamp, String orgId,
            String externalSysType);

    Long findCountByPlayStatesAndTimestamps(Long playId, List<LaunchState> states, Long startTimestamp,
            Long endTimestamp, String orgId, String externalSysType);

    List<Play> findDashboardPlaysWithLaunches(Long playId, List<LaunchState> states, Long startTimestamp,
            Long endTimestamp, String orgId, String externalSysType);

    Stats findTotalCountByPlayStatesAndTimestamps(Long playId, List<LaunchState> states, Long startTimestamp,
            Long endTimestamp, String orgId, String externalSysType);

    List<Pair<String, String>> findDashboardOrgIdWithLaunches(Long playId, List<LaunchState> states,
            Long startTimestamp, Long endTimestamp, String orgId, String externalSysType);

}
