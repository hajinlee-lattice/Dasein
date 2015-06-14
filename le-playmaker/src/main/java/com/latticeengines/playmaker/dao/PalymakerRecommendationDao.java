package com.latticeengines.playmaker.dao;

import java.util.List;
import java.util.Map;

import com.latticeengines.db.exposed.dao.GenericDao;

public interface PalymakerRecommendationDao extends GenericDao {

    List<Map<String, Object>> getRecommendations(int startRecord, int size);

    List<Map<String, Object>> getPlays(int startId, int size);

    List<Map<String, Object>> getAccountExtensions(int startId, int size);

    List<Map<String, Object>> getAccountExtensionSchema();

    List<Map<String, Object>> getPlayValues(int startId, int size);
}
