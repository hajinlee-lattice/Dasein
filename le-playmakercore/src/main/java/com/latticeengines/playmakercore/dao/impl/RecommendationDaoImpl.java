package com.latticeengines.playmakercore.dao.impl;

import java.util.Date;
import java.util.List;
import java.util.Map;

import org.springframework.stereotype.Component;

import com.latticeengines.playmakercore.dao.RecommendationDao;
import com.latticeengines.db.exposed.dao.impl.BaseDaoWithAssignedSessionFactoryImpl;
import com.latticeengines.domain.exposed.playmakercore.Recommendation;

@Component("recommendationDao")
public class RecommendationDaoImpl extends BaseDaoWithAssignedSessionFactoryImpl<Recommendation>
        implements RecommendationDao {

    @Override
    protected Class<Recommendation> getEntityClass() {
        return Recommendation.class;
    }

    @Override
    public List<Recommendation> findByLaunchId(String launchId) {
        // WIP
        return null;
    }

    @Override
    public List<Recommendation> findRecommendations(Date lastModificationDate, Long offset, Long max,
            String syncDestination, List<String> playIds) {
        // WIP
        return null;
    }

    @Override
    public List<Map<String, String>> findRecommendationsAsMap(Date lastModificationDate, Long offset, Long max,
            String syncDestination, List<String> playIds) {
        // WIP
        return null;
    }

    @Override
    public void deleteInBulkByCutoffDate(Date cutoffDate) {
        // WIP
    }

    @Override
    public void deleteByRecommendationId(String recommendationId) {
        // WIP
    }

    @Override
    public void deleteInBulkByLaunchId(String launchId) {
        // WIP
    }

    @Override
    public void deleteInBulkByPlayId(String playId, Date cutoffDate) {
        // WIP
    }

}
