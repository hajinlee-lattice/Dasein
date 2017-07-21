package com.latticeengines.playmakercore.dao.impl;

import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections.CollectionUtils;
import org.hibernate.Query;
import org.hibernate.Session;
import org.springframework.stereotype.Component;

import com.latticeengines.db.exposed.dao.impl.BaseDaoWithAssignedSessionFactoryImpl;
import com.latticeengines.domain.exposed.playmakercore.Recommendation;
import com.latticeengines.playmakercore.dao.RecommendationDao;

@Component("recommendationDao")
public class RecommendationDaoImpl extends BaseDaoWithAssignedSessionFactoryImpl<Recommendation>
        implements RecommendationDao {

    @Override
    protected Class<Recommendation> getEntityClass() {
        return Recommendation.class;
    }

    @SuppressWarnings("unchecked")
    @Override
    public List<Recommendation> findByLaunchId(String launchId) {
        Session session = getSessionFactory().getCurrentSession();
        Class<Recommendation> entityClz = getEntityClass();
        String queryStr = String.format("FROM %s WHERE launchId = :launchId", entityClz.getSimpleName());
        Query query = session.createQuery(queryStr);
        query.setString("launchId", launchId);
        return query.list();
    }

    @SuppressWarnings("unchecked")
    @Override
    public List<Recommendation> findRecommendations(Date lastModificationDate, int offset, int max,
            String syncDestination, List<String> playIds) {
        Session session = getSessionFactory().getCurrentSession();

        Class<Recommendation> entityClz = getEntityClass();
        String queryStr = "FROM %s WHERE synchronizationDestination = :syncDestination ";

        if (!CollectionUtils.isEmpty(playIds)) {
            queryStr += "AND playId IN (:playIds) ";
        }

        queryStr = String.format(queryStr, entityClz.getSimpleName());
        Query query = session.createQuery(queryStr);
        query.setMaxResults(max);
        query.setFirstResult(offset);
        query.setString("syncDestination", syncDestination);
        if (!CollectionUtils.isEmpty(playIds)) {
            query.setParameterList("playIds", playIds);
        }
        return query.list();
    }

    @Override
    public int findRecommendationCount(Date lastModificationDate, String syncDestination, List<String> playIds) {
        Session session = getSessionFactory().getCurrentSession();

        Class<Recommendation> entityClz = getEntityClass();
        String queryStr = "SELECT count(*) FROM %s WHERE synchronizationDestination = :syncDestination ";

        if (!CollectionUtils.isEmpty(playIds)) {
            queryStr += "AND playId IN (:playIds) ";
        }

        queryStr = String.format(queryStr, entityClz.getSimpleName());
        Query query = session.createQuery(queryStr);
        query.setString("syncDestination", syncDestination);
        if (!CollectionUtils.isEmpty(playIds)) {
            query.setParameterList("playIds", playIds);
        }
        return ((Long) query.uniqueResult()).intValue();
    }

    @SuppressWarnings("unchecked")
    @Override
    public List<Map<String, Object>> findRecommendationsAsMap(Date lastModificationDate, int offset, int max,
            String syncDestination, List<String> playIds) {
        Session session = getSessionFactory().getCurrentSession();

        Class<Recommendation> entityClz = getEntityClass();
        String queryStr = "SELECT new map " //
                + "( " //
                + "pid AS ID, recommendationId AS recommendationId, accountId AS accountId, "
                + "leAccountExternalID AS leAccountExternalID, playId AS playId, launchId AS launchId, "
                + "description AS description, launchDate AS launchDate, lastUpdatedTimestamp AS LastModificationDate, "
                + "monetaryValue AS monetaryValue, likelihood AS likelihood, companyName AS companyName, "
                + "sfdcAccountID AS sfdcAccountID, priorityID AS priorityID, priorityDisplayName AS priorityDisplayName, "
                + "monetaryValueIso4217ID AS monetaryValueIso4217ID, contacts AS contacts " + ") " //
                + "FROM %s WHERE synchronizationDestination = :syncDestination ";

        if (!CollectionUtils.isEmpty(playIds)) {
            queryStr += "AND playId IN (:playIds) ";
        }

        queryStr = String.format(queryStr, entityClz.getSimpleName());
        Query query = session.createQuery(queryStr);
        query.setMaxResults(max);
        query.setFirstResult(offset);
        query.setString("syncDestination", syncDestination);
        if (!CollectionUtils.isEmpty(playIds)) {
            query.setParameterList("playIds", playIds);
        }
        return query.list();
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
