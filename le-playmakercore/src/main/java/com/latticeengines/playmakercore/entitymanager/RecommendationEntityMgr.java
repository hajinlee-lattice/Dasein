package com.latticeengines.playmakercore.entitymanager;

import java.util.Date;
import java.util.List;
import java.util.Map;

import com.latticeengines.db.exposed.entitymgr.BaseEntityMgr;
import com.latticeengines.domain.exposed.playmakercore.Recommendation;

public interface RecommendationEntityMgr extends BaseEntityMgr<Recommendation> {

    void create(Recommendation entity);

    Recommendation findByRecommendationId(String recommendationId);

    List<Recommendation> findByLaunchId(String launchId);

    List<Recommendation> findRecommendations(Date lastModificationDate, //
            int offset, int max, String syncDestination, List<String> playIds, Map<String, String> orgInfo);

    int findRecommendationCount(Date lastModificationDate, //
            String syncDestination, List<String> playIds, Map<String, String> orgInfo);

    List<Map<String, Object>> findRecommendationsAsMap(Date lastModificationDate, //
            int offset, int max, String syncDestination, List<String> playIds, Map<String, String> orgInfo);

    int deleteInBulkByCutoffDate(Date cutoffDate, boolean hardDelete, int maxUpdateRows);

    void delete(Recommendation entity, boolean hardDelete);

    int deleteInBulkByLaunchId(String launchId, boolean hardDelete, int maxUpdateRows);

    int deleteInBulkByPlayId(String playId, Date cutoffDate, boolean hardDelete, int maxUpdateRows);
}
