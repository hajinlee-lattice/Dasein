package com.latticeengines.playmaker.dao;

import java.util.List;
import java.util.Map;

public interface LpiPMRecommendationDao {

    List<Map<String, Object>> getRecommendations(long start, int offset, int maximum, int syncDestination,
            List<String> playIds);

    int getRecommendationCount(long start, int syncDestination, List<String> playIds);
}
