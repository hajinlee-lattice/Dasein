package com.latticeengines.pls.service.impl;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.commons.collections.CollectionUtils;
import org.codehaus.plexus.util.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.pls.LaunchState;
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
            Long offset, Long max, Long endTimestamp) {
        PlayLaunchDashboard dashboard = new PlayLaunchDashboard();
        List<PlayLaunch> playLaunches = playLaunchEntityMgr.findDashboardEntries(playId, launchStates, startTimestamp,
                offset, max, endTimestamp);
        Stats totalCounts = playLaunchEntityMgr.findDashboardCumulativeStats(playId, launchStates, startTimestamp,
                endTimestamp);

        List<LaunchSummary> launchSummaries = convertToSummaries(playLaunches);

        dashboard.setLaunchSummaries(launchSummaries);
        dashboard.setCumulativeStats(totalCounts);
        return dashboard;
    }

    @Override
    public Long getDashboardEntriesCount(Long playId, List<LaunchState> launchStates, Long startTimestamp,
            Long endTimestamp) {
        return playLaunchEntityMgr.findDashboardEntriesCount(playId, launchStates, startTimestamp, endTimestamp);
    }

    private List<LaunchSummary> convertToSummaries(List<PlayLaunch> playLaunches) {
        if (CollectionUtils.isEmpty(playLaunches)) {
            return new ArrayList<>();
        } else {
            return playLaunches.stream() //
                    .map(launch -> convertToSummary(launch)) //
                    .collect(Collectors.toList());
        }
    }

    private LaunchSummary convertToSummary(PlayLaunch launch) {
        LaunchSummary summary = new LaunchSummary();

        Stats stats = new Stats();
        stats.setContactsWithinRecommendations(getCount(launch.getContactsLaunched()));
        stats.setErrors(getCount(launch.getAccountsErrored()));
        stats.setRecommendationsLaunched(getCount(launch.getAccountsLaunched()));
        stats.setSuppressed(getCount(launch.getAccountsSuppressed()));

        summary.setStats(stats);
        summary.setLaunchId(launch.getLaunchId());
        summary.setLaunchState(launch.getLaunchState());
        summary.setLaunchTime(launch.getCreated());
        summary.setSelectedBuckets(launch.getBucketsToLaunch());

        return summary;
    }

    private long getCount(Long count) {
        return count == null ? 0L : count;
    }
}
