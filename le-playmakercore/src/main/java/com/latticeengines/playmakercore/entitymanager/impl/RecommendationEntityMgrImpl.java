package com.latticeengines.playmakercore.entitymanager.impl;

import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import javax.inject.Inject;

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

    @Inject
    private RecommendationDao recommendationDao;

    @Override
    public BaseDao<Recommendation> getDao() {
        return recommendationDao;
    }

    @Transactional(value = "datadb", propagation = Propagation.REQUIRED)
    @Override
    public void createOrUpdate(Recommendation entity) {
        getDao().createOrUpdate(entity);
    }

    @Transactional(value = "datadb", propagation = Propagation.REQUIRED)
    @Override
    public void update(Recommendation entity) {
        getDao().update(entity);
    }

    @Transactional(value = "datadb", propagation = Propagation.REQUIRED)
    @Override
    public void delete(Recommendation entity, boolean hardDelete) {
        getDao().deleteByPid(entity.getPid(), hardDelete);
    }

    @Transactional(value = "datadb", propagation = Propagation.REQUIRED)
    @Override
    public void deleteAll() {
        getDao().deleteAll();
    }

    @Transactional(value = "datadb", propagation = Propagation.REQUIRED, readOnly = true)
    @Override
    public boolean containInSession(Recommendation entity) {
        return getDao().containInSession(entity);
    }

    /**
     * get object by key. entity.getPid() must NOT be empty.
     */
    @Transactional(value = "datadb", propagation = Propagation.REQUIRED, readOnly = true)
    @Override
    public Recommendation findByKey(Recommendation entity) {
        return getDao().findByKey(entity);
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public Recommendation findByField(String fieldName, Object value) {
        return getDao().findByField(fieldName, value);
    }

    @Override
    @Transactional(value = "datadb", propagation = Propagation.REQUIRED, readOnly = false)
    public void create(Recommendation entity) {
        Date timestamp = new Date(System.currentTimeMillis());

        entity.setLastUpdatedTimestamp(timestamp);
        entity.setRecommendationId(generateRecommendationId());
        recommendationDao.create(entity);
    }

    @Override
    @Transactional(value = "datadb", propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public List<Recommendation> findAll() {
        return getDao().findAll();
    }

    @Override
    @Transactional(value = "datadb", propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public Recommendation findByRecommendationId(String recommendationId) {
        return recommendationDao.findByRecommendationId(recommendationId);
    }

    @Override
    @Transactional(value = "datadb", propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public List<Recommendation> findByLaunchId(String launchId) {
        return recommendationDao.findByLaunchId(launchId);
    }

    @Override
    @Transactional(value = "datadb", propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public List<Recommendation> findByLaunchIds(List<String> launchIds) {
        return recommendationDao.findByLaunchIds(launchIds);
    }

    private String generateRecommendationId() {
        return String.format(PLAY_LAUNCH_NAME_FORMAT, PLAY_LAUNCH_NAME_PREFIX, UUID.randomUUID().toString());
    }

    @Override
    @Transactional(value = "datadb", propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public List<Recommendation> findRecommendations(Date lastModificationDate, int offset, int max,
            String syncDestination, List<String> playIds, Map<String, String> orgInfo) {
        return recommendationDao.findRecommendations(lastModificationDate, offset, max, syncDestination, playIds,
                orgInfo);
    }

    @Override
    @Transactional(value = "datadb", propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public int findRecommendationCount(Date lastModificationDate, String syncDestination, List<String> playIds,
            Map<String, String> orgInfo) {
        return recommendationDao.findRecommendationCount(lastModificationDate, syncDestination, playIds, orgInfo);
    }

    @Override
    @Transactional(value = "datadb", propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public int findRecommendationCountByLaunchIds(List<String> launchIds, long start) {
        return recommendationDao.findRecommendationCountByLaunchIds(launchIds);
    }

    @Override
    @Transactional(value = "datadb", propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public List<Map<String, Object>> findRecommendationsAsMap(Date lastModificationDate, int offset, int max,
            String syncDestination, List<String> playIds, Map<String, String> orgInfo) {
        return recommendationDao.findRecommendationsAsMap(lastModificationDate, offset, max, syncDestination, playIds,
                orgInfo);
    }

    @Override
    @Transactional(value = "datadb", propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public List<Map<String, Object>> findRecommendationsAsMapByLaunchIds(List<String> launchIds, long start, int offset, int max) {
        return recommendationDao.findRecommendationsAsMapByLaunchIds(launchIds, start, offset, max);
    }

    @Override
    @Transactional(value = "datadb", propagation = Propagation.REQUIRES_NEW, readOnly = false)
    public int deleteInBulkByCutoffDate(Date cutoffDate, boolean hardDelete, int maxUpdateRows) {
        return recommendationDao.deleteInBulkByCutoffDate(cutoffDate, hardDelete, maxUpdateRows);
    }

    @Override
    @Transactional(value = "datadb", propagation = Propagation.REQUIRES_NEW, readOnly = false)
    public int deleteInBulkByLaunchId(String launchId, boolean hardDelete, int maxUpdateRows) {
        return recommendationDao.deleteInBulkByLaunchId(launchId, hardDelete, maxUpdateRows);
    }

    @Override
    @Transactional(value = "datadb", propagation = Propagation.REQUIRES_NEW, readOnly = false)
    public int deleteInBulkByPlayId(String playId, Date cutoffDate, boolean hardDelete, int maxUpdateRows) {
        return recommendationDao.deleteInBulkByPlayId(playId, cutoffDate, hardDelete, maxUpdateRows);
    }

    @Override
    @Transactional(value = "datadb", propagation = Propagation.REQUIRES_NEW, readOnly = false)
    public void delete(Recommendation entity) {
        getDao().deleteByPid(entity.getPid(), false);
    }

    @Override
    @Transactional(value = "datadb", propagation = Propagation.REQUIRES_NEW, readOnly = false)
    public List<Map<String, Object>> findAccountIdsFromRecommendationByLaunchId(List<String> launchIds, long start, int offset, int max) {
        return recommendationDao.findAccountIdsByLaunchIds(launchIds, start, offset, max);
    }

    @Override
    @Transactional(value = "datadb", propagation = Propagation.REQUIRES_NEW, readOnly = false)
    public int findAccountIdsCountFromRecommendationByLaunchId(List<String> launchIds, long start) {
        return recommendationDao.findAccountIdCountByLaunchIds(launchIds, start);
    }

    @Override
    @Transactional(value = "datadb", propagation = Propagation.REQUIRES_NEW, readOnly = false)
    public List<Map<String, Object>> findContactsByLaunchIds(List<String> launchIds, long start, int offset, int maximum, List<String> accountIds) {
        return recommendationDao.findContactsByLaunchIds(launchIds, start, offset, maximum, accountIds);
    }

}
