package com.latticeengines.datadb.playmaker.dao;

import java.util.Date;
import java.util.List;
import java.util.Map;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.domain.exposed.datadb.playmaker.Recommendation;

public interface RecommendationDao extends BaseDao<Recommendation> {

    List<Recommendation> findByLaunchId(String launchId);

    List<Recommendation> findRecommendations(Date lastModificationDate, //
            Long offset, Long max, String syncDestination, List<String> playIds);

    List<Map<String, String>> findRecommendationsAsMap(Date lastModificationDate, //
            Long offset, Long max, String syncDestination, List<String> playIds);

    void deleteInBulkByCutoffDate(Date cutoffDate);

    void deleteByRecommendationId(String recommendationId);

    void deleteInBulkByLaunchId(String launchId);

    void deleteInBulkByPlayId(String playId, Date cutoffDate);

}
