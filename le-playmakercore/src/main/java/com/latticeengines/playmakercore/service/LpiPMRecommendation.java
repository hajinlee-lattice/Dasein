package com.latticeengines.playmakercore.service;

import java.util.Date;
import java.util.List;
import java.util.Map;

import com.latticeengines.domain.exposed.playmakercore.Recommendation;
import com.latticeengines.domain.exposed.playmakercore.SynchronizationDestinationEnum;

public interface LpiPMRecommendation {
    List<Map<String, Object>> getRecommendations(long start, int offset, int maximum,
            SynchronizationDestinationEnum syncDestination, List<String> playIds, Map<String, String> orgInfo,
            Map<String, String> appId);

    List<Map<String, Object>> getRecommendationsByLaunchIds(List<String> launchIds, int offset, int maximum);

    int getRecommendationCount(long start, SynchronizationDestinationEnum syncDestination, List<String> playIds,
            Map<String, String> orgInfo, Map<String, String> appId);

    int getRecommendationCountByLaunchIds(List<String> launchIds);

    Recommendation getRecommendationById(String recommendationId);

    int cleanupRecommendations(String playId);

    int cleanupOldRecommendationsBeforeCutoffDate(Date cutoffDate);

    List<String> getAccountIdsFromRecommendationByLaunchId(List<String> launchIds, int offset, int max);

    int getAccountIdsCountFromRecommendationByLaunchId(List<String> launchIds);
}
