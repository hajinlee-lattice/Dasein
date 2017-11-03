package com.latticeengines.playmakercore.dao.impl;

import java.math.BigInteger;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections4.CollectionUtils;
import org.hibernate.Query;
import org.hibernate.Session;
import org.springframework.stereotype.Component;

import com.latticeengines.db.exposed.dao.impl.BaseDaoWithAssignedSessionFactoryImpl;
import com.latticeengines.domain.exposed.playmaker.PlaymakerConstants;
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
        String queryStr = "FROM %s WHERE synchronizationDestination = :syncDestination " //
                + "AND UNIX_TIMESTAMP(lastUpdatedTimestamp) >= :lastUpdatedTimestamp ";

        if (!CollectionUtils.isEmpty(playIds)) {
            queryStr += "AND playId IN (:playIds) ";
        }

        queryStr = String.format(queryStr, entityClz.getSimpleName());
        Query query = session.createQuery(queryStr);
        query.setMaxResults(max);
        query.setFirstResult(offset);
        query.setString("syncDestination", syncDestination);

        updateQueryWithLastUpdatedTimestamp(lastModificationDate, query);

        if (!CollectionUtils.isEmpty(playIds)) {
            query.setParameterList("playIds", playIds);
        }
        return query.list();
    }

    void updateQueryWithLastUpdatedTimestamp(Date lastModificationDate, Query query) {
        if (lastModificationDate == null) {
            lastModificationDate = new Date(0L);
        }
        query.setBigInteger("lastUpdatedTimestamp",
                new BigInteger((dateToUnixTimestamp(lastModificationDate).toString())));
    }

    Long dateToUnixTimestamp(Date lastModificationDate) {
        return new Long(lastModificationDate.getTime() / 1000);
    }

    @Override
    public int findRecommendationCount(Date lastModificationDate, String syncDestination, List<String> playIds) {
        Session session = getSessionFactory().getCurrentSession();

        Class<Recommendation> entityClz = getEntityClass();
        String queryStr = "SELECT count(*) FROM %s WHERE synchronizationDestination = :syncDestination " //
                + "AND UNIX_TIMESTAMP(lastUpdatedTimestamp) >= :lastUpdatedTimestamp ";

        if (!CollectionUtils.isEmpty(playIds)) {
            queryStr += "AND playId IN (:playIds) ";
        }

        queryStr += " ORDER BY lastUpdatedTimestamp ";
        queryStr = String.format(queryStr, entityClz.getSimpleName());
        Query query = session.createQuery(queryStr);
        query.setString("syncDestination", syncDestination);

        updateQueryWithLastUpdatedTimestamp(lastModificationDate, query);

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
                + "  recommendationId AS " + PlaymakerConstants.ID //
                + ", accountId AS " + PlaymakerConstants.AccountID //
                + ", leAccountExternalID AS " + PlaymakerConstants.LEAccountExternalID //
                + ", playId AS " + PlaymakerConstants.PlayID //
                + ", launchId AS " + PlaymakerConstants.LaunchID //
                + ", description AS " + PlaymakerConstants.Description //
                + ", UNIX_TIMESTAMP(launchDate) AS " + PlaymakerConstants.LaunchDate //
                + ", UNIX_TIMESTAMP(lastUpdatedTimestamp) AS " + PlaymakerConstants.LastModificationDate //
                + ", monetaryValue AS " + PlaymakerConstants.MonetaryValue //
                + ", likelihood AS " + PlaymakerConstants.Likelihood //
                + ", companyName AS " + PlaymakerConstants.CompanyName //
                + ", sfdcAccountID AS " + PlaymakerConstants.SfdcAccountID //
                + ", priorityID AS " + PlaymakerConstants.PriorityID //
                + ", priorityDisplayName AS " + PlaymakerConstants.PriorityDisplayName //
                + ", monetaryValueIso4217ID AS " + PlaymakerConstants.MonetaryValueIso4217ID //
                + ", contacts AS " + PlaymakerConstants.Contacts //
                + ") " //
                + "FROM %s " //
                + "WHERE synchronizationDestination = :syncDestination " //
                + "AND UNIX_TIMESTAMP(lastUpdatedTimestamp) >= :lastUpdatedTimestamp ";

        if (!CollectionUtils.isEmpty(playIds)) {
            queryStr += "AND playId IN (:playIds) ";
        }

        queryStr += " ORDER BY lastUpdatedTimestamp ";
        queryStr = String.format(queryStr, entityClz.getSimpleName());

        Query query = session.createQuery(queryStr);
        query.setMaxResults(max);
        query.setFirstResult(offset);
        query.setString("syncDestination", syncDestination);

        updateQueryWithLastUpdatedTimestamp(lastModificationDate, query);

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
