package com.latticeengines.playmaker.service;

import java.util.List;
import java.util.Map;

import com.latticeengines.domain.exposed.pls.LaunchSummary;
import com.latticeengines.domain.exposed.pls.Play;

public interface LpiPMPlay {

    List<Map<String, Object>> getPlays(long start, int offset, int maximum, List<Integer> playgroupIds,
            int syncDestination, Map<String, String> orgInfo);

    List<Play> getPlayList(long start, List<Integer> playgroupIds, int syncDestination, Map<String, String> orgInfo);

    int getPlayCount(long start, List<Integer> playgroupIds, int syncDestination, Map<String, String> orgInfo);

    List<String> getLaunchIdsFromDashboard(boolean latest, long start, List<String> playIds, int syncDestination,
            Map<String, String> orgInfo);

    List<LaunchSummary> getLaunchSummariesFromDashboard(boolean latest, long start, List<String> playIds,
            int syncDestination, Map<String, String> orgInfo);
}
