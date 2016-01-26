package com.latticeengines.playmaker.dao;

import java.util.List;
import java.util.Map;

import com.latticeengines.db.exposed.dao.GenericDao;

public interface PlaymakerRecommendationDao extends GenericDao {

    List<Map<String, Object>> getRecommendations(long start, int offset, int maximum, int syncDestination);

    List<Map<String, Object>> getPlays(long start, int offset, int maximum);

    List<Map<String, Object>> getAccountExtensions(long start, int offset, int maximum);

    List<Map<String, Object>> getAccountExtensionSchema();

    List<Map<String, Object>> getPlayValues(long start, int offset, int maximum);

    List<Map<String, Object>> getWorkflowTypes();

    int getRecommendationCount(long start, int syncDestination);

    int getPlayCount(long start);

    int getAccountExtensionCount(long start);

    int getPlayValueCount(long start);

    int getAccountExtensionColumnCount();

}
