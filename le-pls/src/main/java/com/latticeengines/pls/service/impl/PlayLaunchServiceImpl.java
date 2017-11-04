package com.latticeengines.pls.service.impl;

import java.util.Date;
import java.util.List;

import org.codehaus.plexus.util.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.pls.LaunchState;
import com.latticeengines.domain.exposed.pls.Play;
import com.latticeengines.domain.exposed.pls.PlayLaunch;
import com.latticeengines.domain.exposed.pls.PlayLaunchDashboard;
import com.latticeengines.domain.exposed.pls.PlayLaunchDashboard.LaunchSummary;
import com.latticeengines.domain.exposed.pls.PlayLaunchDashboard.Stats;
import com.latticeengines.pls.entitymanager.PlayLaunchEntityMgr;
import com.latticeengines.pls.service.PlayLaunchService;

@Component("playLaunchService")
public class PlayLaunchServiceImpl implements PlayLaunchService {
    @Autowired
    private PlayLaunchEntityMgr playLaunchEntityMgr;

    @Override
    public void create(PlayLaunch entity) {
        playLaunchEntityMgr.create(entity);
    }

    @Override
    public PlayLaunch findByLaunchId(String launchId) {
        if (StringUtils.isBlank(launchId)) {
            throw new LedpException(LedpCode.LEDP_18146);
        }
        return playLaunchEntityMgr.findByLaunchId(launchId);
    }

    @Override
    public void deleteByLaunchId(String launchId) {
        if (StringUtils.isBlank(launchId)) {
            throw new LedpException(LedpCode.LEDP_18146);
        }
        playLaunchEntityMgr.deleteByLaunchId(launchId);
    }

    @Override
    public PlayLaunch findByPlayAndTimestamp(Long playId, Date timestamp) {
        return playLaunchEntityMgr.findByPlayAndTimestamp(playId, timestamp);
    }

    @Override
    public List<PlayLaunch> findByPlayId(Long playId, List<LaunchState> states) {
        return playLaunchEntityMgr.findByPlayId(playId, states);
    }

    @Override
    public PlayLaunch findLatestByPlayId(Long playId, List<LaunchState> launchStates) {
        return playLaunchEntityMgr.findLatestByPlayId(playId, launchStates);
    }

    @Override
    public List<PlayLaunch> findByState(LaunchState state) {
        return playLaunchEntityMgr.findByState(state);
    }

    @Override
    public PlayLaunch update(PlayLaunch playLaunch) {
        playLaunchEntityMgr.update(playLaunch);
        return playLaunchEntityMgr.findByKey(playLaunch);
    }

    @Override
    public PlayLaunchDashboard getDashboard(Long playId, List<LaunchState> launchStates, Long startTimestamp,
            Long offset, Long max, String sortby, boolean descending, Long endTimestamp) {
        PlayLaunchDashboard dashboard = new PlayLaunchDashboard();
        Stats totalCounts = playLaunchEntityMgr.findDashboardCumulativeStats(playId, launchStates, startTimestamp,
                endTimestamp);

        List<Play> uniquePlaysWithLaunches = playLaunchEntityMgr.findDashboardPlaysWithLaunches(playId, launchStates,
                startTimestamp, endTimestamp);

        List<LaunchSummary> launchSummaries = playLaunchEntityMgr.findDashboardEntries(playId, launchStates,
                startTimestamp, offset, max, sortby, descending, endTimestamp);

        dashboard.setLaunchSummaries(launchSummaries);
        dashboard.setCumulativeStats(totalCounts);
        dashboard.setUniquePlaysWithLaunches(uniquePlaysWithLaunches);
        return dashboard;
    }

    @Override
    public Long getDashboardEntriesCount(Long playId, List<LaunchState> launchStates, Long startTimestamp,
            Long endTimestamp) {
        return playLaunchEntityMgr.findDashboardEntriesCount(playId, launchStates, startTimestamp, endTimestamp);
    }
}
