package com.latticeengines.playmakercore.entitymanager;

import java.util.Date;
import java.util.List;
import java.util.Map;

import com.latticeengines.db.exposed.entitymgr.BaseEntityMgr;
import com.latticeengines.domain.exposed.playmakercore.Recommendation;

public interface RecommendationEntityMgr extends BaseEntityMgr<Recommendation> {

    void create(Recommendation entity);

    List<Recommendation> findByLaunchId(String launchId);

    List<Recommendation> findRecommendations(Date lastModificationDate, //
            int offset, int max, String syncDestination, List<String> playIds);

    int findRecommendationCount(Date lastModificationDate, //
            String syncDestination, List<String> playIds);

    List<Map<String, Object>> findRecommendationsAsMap(Date lastModificationDate, //
            int offset, int max, String syncDestination, List<String> playIds);

    void deleteInBulkByCutoffDate(Date cutoffDate);

    void delete(Recommendation entity);

    void deleteInBulkByLaunchId(String launchId);

    void deleteInBulkByPlayId(String playId, Date cutoffDate);
}
