package com.latticeengines.datadb.playmaker.entitymanager.impl;

import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.datadb.playmaker.dao.RecommendationDao;
import com.latticeengines.datadb.playmaker.entitymanager.RecommendationEntityMgr;
import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.db.exposed.entitymgr.impl.BaseEntityMgrImpl;
import com.latticeengines.domain.exposed.datadb.playmaker.Recommendation;

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

        entity.setCreatedTimestamp(timestamp);
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
    public void deleteInBulkByLaunchId(String launchId) {
        // WIP

    }

    @Override
    public void deleteInBulkByPlayId(String playId, Date cutoffDate) {
        // WIP

    }
}
