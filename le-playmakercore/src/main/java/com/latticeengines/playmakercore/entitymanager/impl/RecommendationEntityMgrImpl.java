package com.latticeengines.playmakercore.entitymanager.impl;

import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.db.exposed.entitymgr.impl.BaseEntityMgrImpl;
import com.latticeengines.domain.exposed.playmakercore.Recommendation;
import com.latticeengines.playmakercore.dao.RecommendationDao;
import com.latticeengines.playmakercore.entitymanager.RecommendationEntityMgr;

@Component("recommendationEntityMgr")
public class RecommendationEntityMgrImpl extends BaseEntityMgrImpl<Recommendation> implements RecommendationEntityMgr {

    private static final String PLAY_LAUNCH_NAME_PREFIX = "recmm";
    private static final String PLAY_LAUNCH_NAME_FORMAT = "%s__%s";

    @Autowired
    private RecommendationDao recommendationDao;

    @Override
    public BaseDao<Recommendation> getDao() {
        return recommendationDao;
    }

    @Override
    @Transactional(value = "dataTransactionManager", propagation = Propagation.REQUIRED, readOnly = false)
    public void create(Recommendation entity) {
        Date timestamp = new Date(System.currentTimeMillis());

        entity.setLastUpdatedTimestamp(timestamp);
        entity.setRecommendationId(generateRecommendationId());
        recommendationDao.create(entity);
    }

    @Override
    @Transactional(value = "dataTransactionManager", propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public List<Recommendation> findAll() {
        return super.findAll();
    }

    @Override
    @Transactional(value = "dataTransactionManager", propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public List<Recommendation> findByLaunchId(String launchId) {
        return recommendationDao.findByLaunchId(launchId);
    }

    private String generateRecommendationId() {
        return String.format(PLAY_LAUNCH_NAME_FORMAT, PLAY_LAUNCH_NAME_PREFIX, UUID.randomUUID().toString());
    }

    @Override
    @Transactional(value = "dataTransactionManager", propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public List<Recommendation> findRecommendations(Date lastModificationDate, int offset, int max,
            String syncDestination, List<String> playIds) {
        return recommendationDao.findRecommendations(lastModificationDate, offset, max, syncDestination, playIds);
    }

    @Override
    @Transactional(value = "dataTransactionManager", propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public int findRecommendationCount(Date lastModificationDate, String syncDestination, List<String> playIds) {
        return recommendationDao.findRecommendationCount(lastModificationDate, syncDestination, playIds);
    }

    @Override
    @Transactional(value = "dataTransactionManager", propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public List<Map<String, Object>> findRecommendationsAsMap(Date lastModificationDate, int offset, int max,
            String syncDestination, List<String> playIds) {
        return recommendationDao.findRecommendationsAsMap(lastModificationDate, offset, max, syncDestination, playIds);
    }

    @Override
    @Transactional(value = "dataTransactionManager", propagation = Propagation.REQUIRES_NEW, readOnly = false)
    public void deleteInBulkByCutoffDate(Date cutoffDate) {
        // WIP

    }

    @Override
    @Transactional(value = "dataTransactionManager", propagation = Propagation.REQUIRES_NEW, readOnly = false)
    public void deleteInBulkByLaunchId(String launchId) {
        // WIP

    }

    @Override
    @Transactional(value = "dataTransactionManager", propagation = Propagation.REQUIRES_NEW, readOnly = false)
    public void deleteInBulkByPlayId(String playId, Date cutoffDate) {
        // WIP

    }
}
