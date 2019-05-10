package com.latticeengines.playmakercore.dao;

import java.util.Date;
import java.util.List;
import java.util.Map;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.domain.exposed.playmakercore.Recommendation;

public interface RecommendationDao extends BaseDao<Recommendation> {

    Recommendation findByRecommendationId(String recommendationId);

    List<Recommendation> findByLaunchId(String launchId);

    List<Recommendation> findByLaunchIds(List<String> launchIds);

    int findRecommendationCountByLaunchIds(List<String> launchIds);

    List<Recommendation> findRecommendations(Date lastModificationDate, //
            int offset, int max, String syncDestination, List<String> playIds, Map<String, String> orgInfo);

    int findRecommendationCount(Date lastModificationDate, String syncDestination, List<String> playIds,
            Map<String, String> orgInfo);

    List<Map<String, Object>> findAccountIdsByLaunchIds(List<String> launchIds, long start, int offset, int max);

//    List<String> findAccountIdsByLaunchIds(List<String> launchIds);

    int findAccountIdCountByLaunchIds(List<String> launchIds, long start);

    List<Map<String, Object>> findRecommendationsAsMap(Date lastModificationDate, //
            int offset, int max, String syncDestination, List<String> playIds, Map<String, String> orgInfo);

    int deleteInBulkByCutoffDate(Date cutoffDate, boolean hardDelete, int maxUpdateRows);

    void deleteByRecommendationId(String recommendationId, boolean hardDelete);

    int deleteInBulkByLaunchId(String launchId, boolean hardDelete, int maxUpdateRows);

    int deleteInBulkByPlayId(String playId, Date cutoffDate, boolean hardDelete, int maxUpdateRows);

    List<Map<String, Object>> findContactsByLaunchIds(List<String> launchIds, long start, int offset, int maximum, List<String> accountIds);

    List<Map<String, Object>> findRecommendationsAsMapByLaunchIds(List<String> launchIds, long start, int offset, int max);

}
