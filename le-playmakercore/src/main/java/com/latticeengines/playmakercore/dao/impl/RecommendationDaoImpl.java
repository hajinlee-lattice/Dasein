package com.latticeengines.playmakercore.dao.impl;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.hibernate.Session;
import org.hibernate.query.Query;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.db.exposed.dao.impl.BaseDaoWithAssignedSessionFactoryImpl;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.cdl.CDLConstants;
import com.latticeengines.domain.exposed.playmaker.PlaymakerConstants;
import com.latticeengines.domain.exposed.playmaker.PlaymakerUtils;
import com.latticeengines.domain.exposed.playmakercore.Recommendation;
import com.latticeengines.domain.exposed.pls.LookupIdMapUtils;
import com.latticeengines.domain.exposed.pls.PlayLaunchDashboard;
import com.latticeengines.domain.exposed.pls.PlayLaunchDashboard.LaunchSummary;
import com.latticeengines.playmakercore.dao.RecommendationDao;
import com.latticeengines.proxy.exposed.cdl.PlayProxy;

@Component("recommendationDao")
public class RecommendationDaoImpl extends BaseDaoWithAssignedSessionFactoryImpl<Recommendation>
        implements RecommendationDao {

    private static final Logger log = LoggerFactory.getLogger(RecommendationDaoImpl.class);

    @Override
    protected Class<Recommendation> getEntityClass() {
        return Recommendation.class;
    }

    @Override
    public Recommendation findByRecommendationId(String recommendationId) {
        Session session = getSessionFactory().getCurrentSession();
        Class<Recommendation> entityClz = getEntityClass();
        String queryStr = String.format("FROM %s " //
                + "WHERE recommendationId = :recommendationId " //
                + "AND deleted = :deleted ", entityClz.getSimpleName());
        Query<?> query = session.createQuery(queryStr);
        query.setParameter("recommendationId", recommendationId);
        query.setParameter("deleted", Boolean.FALSE);
        return (Recommendation) query.uniqueResult();
    }

    @SuppressWarnings("unchecked")
    @Override
    public List<Recommendation> findByLaunchId(String launchId) {
        Session session = getSessionFactory().getCurrentSession();
        Class<Recommendation> entityClz = getEntityClass();
        String queryStr = String.format("FROM %s " //
                + "WHERE launchId = :launchId " //
                + "AND deleted = :deleted ", entityClz.getSimpleName());
        Query<Recommendation> query = session.createQuery(queryStr);
        query.setParameter("launchId", launchId);
        query.setParameter("deleted", Boolean.FALSE);
        return query.list();
    }

    @SuppressWarnings("unchecked")
    @Override
    public List<Recommendation> findByLaunchIds(List<String> launchIds) {
        Session session = getSessionFactory().getCurrentSession();
        Class<Recommendation> entityClz = getEntityClass();
        String queryStr = String.format("FROM %s " //
                + "WHERE launchId in (:launchIds) " //
                + "AND deleted = :deleted ORDER BY recommendationId", entityClz.getSimpleName());
        Query<Recommendation> query = session.createQuery(queryStr);
        query.setParameterList("launchIds", launchIds);
        query.setParameter("deleted", Boolean.FALSE);
        return query.list();
    }

    @SuppressWarnings("unchecked")
    @Override
    public int findRecommendationCountByLaunchIds(List<String> launchIds) {
        Session session = getSessionFactory().getCurrentSession();
        Class<Recommendation> entityClz = getEntityClass();
        String queryStr = "SELECT count(*) " //
                + "FROM %s " //
                + "WHERE launchId in (:launchIds) " //
                + "AND deleted = :deleted ";
        queryStr = String.format(queryStr, entityClz.getSimpleName());
        Query<?> query = session.createQuery(queryStr);
        query.setParameterList("launchIds", launchIds);
        query.setParameter("deleted", Boolean.FALSE);
        return ((Long) query.uniqueResult()).intValue();
    }

    @SuppressWarnings("unchecked")
    @Override
    public List<Recommendation> findRecommendations(Date lastModificationDate, int offset, int max,
            String syncDestination, List<String> playIds, Map<String, String> orgInfo) {
        Session session = getSessionFactory().getCurrentSession();

        Pair<String, String> effectiveOrgInfo = LookupIdMapUtils.getEffectiveOrgInfo(orgInfo);

        Class<Recommendation> entityClz = getEntityClass();
        String queryStr = "FROM %s " //
                + "WHERE synchronizationDestination = :syncDestination " //
                + "AND UNIX_TIMESTAMP(lastUpdatedTimestamp) >= :lastUpdatedTimestamp " //
                + "AND deleted = :deleted ";

        queryStr = additionalWhereClause(playIds, effectiveOrgInfo, queryStr);

        queryStr = String.format(queryStr, entityClz.getSimpleName());
        Query<Recommendation> query = session.createQuery(queryStr);
        query.setMaxResults(max);
        query.setFirstResult(offset);
        query.setParameter("syncDestination", syncDestination);

        updateQueryWithLastUpdatedTimestamp(lastModificationDate, query);

        setParamValues(playIds, effectiveOrgInfo, query);
        query.setParameter("deleted", Boolean.FALSE);

        return query.list();
    }

    @SuppressWarnings("deprecation")
    void updateQueryWithLastUpdatedTimestamp(Date lastModificationDate, Query<?> query) {
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
    public int findRecommendationCount(Date lastModificationDate, String syncDestination, List<String> playIds,
            Map<String, String> orgInfo) {
        Session session = getSessionFactory().getCurrentSession();

        Pair<String, String> effectiveOrgInfo = LookupIdMapUtils.getEffectiveOrgInfo(orgInfo);

        Class<Recommendation> entityClz = getEntityClass();
        String queryStr = "SELECT count(*) " //
                + "FROM %s " //
                + "WHERE synchronizationDestination = :syncDestination " //
                + "AND UNIX_TIMESTAMP(lastUpdatedTimestamp) >= :lastUpdatedTimestamp " //
                + "AND deleted = :deleted ";

        queryStr = additionalWhereClause(playIds, effectiveOrgInfo, queryStr);

        queryStr += " ORDER BY lastUpdatedTimestamp ";
        queryStr = String.format(queryStr, entityClz.getSimpleName());
        Query<?> query = session.createQuery(queryStr);
        query.setParameter("syncDestination", syncDestination);

        updateQueryWithLastUpdatedTimestamp(lastModificationDate, query);

        setParamValues(playIds, effectiveOrgInfo, query);
        query.setParameter("deleted", Boolean.FALSE);
        return ((Long) query.uniqueResult()).intValue();
    }

    // private List<String> findAccountIdsByLaunchId(List<String> launchId) {
    // Session session = getSessionFactory().getCurrentSession();
    // Class<Recommendation> entityClz = getEntityClass();
    // String queryStr = String.format("FROM %s " //
    // + "WHERE launchId = :launchId " //
    // + "AND deleted = :deleted ", entityClz.getSimpleName());
    // Query<Recommendation> query = session.createQuery(queryStr);
    // query.setParameter("launchId", launchId);
    // if (!CollectionUtils.isEmpty(playIds)) {
    // query.setParameterList("playIds", playIds);
    // }
    // query.setParameter("deleted", Boolean.FALSE);
    // return query.list();
    // }

    @SuppressWarnings("unchecked")
    @Override
    public List<Map<String, Object>> findRecommendationsAsMap(Date lastModificationDate, int offset, int max,
            String syncDestination, List<String> playIds, Map<String, String> orgInfo) {
        Session session = getSessionFactory().getCurrentSession();

        Pair<String, String> effectiveOrgInfo = LookupIdMapUtils.getEffectiveOrgInfo(orgInfo);

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
                + ", lift AS " + PlaymakerConstants.Lift//
                + ", ratingModelId AS " + PlaymakerConstants.RatingModelId //
                + ", modelSummaryId AS " + PlaymakerConstants.ModelSummaryId //
                + ") " //
                + "FROM %s " //
                + "WHERE synchronizationDestination = :syncDestination " //
                + "AND UNIX_TIMESTAMP(lastUpdatedTimestamp) >= :lastUpdatedTimestamp " //
                + "AND deleted = :deleted ";

        queryStr = additionalWhereClause(playIds, effectiveOrgInfo, queryStr);

        queryStr += " ORDER BY lastUpdatedTimestamp ";
        queryStr = String.format(queryStr, entityClz.getSimpleName());

        @SuppressWarnings("rawtypes")
        Query query = session.createQuery(queryStr);
        query.setMaxResults(max);
        query.setFirstResult(offset);
        query.setParameter("syncDestination", syncDestination);
        query.setParameter("deleted", Boolean.FALSE);

        updateQueryWithLastUpdatedTimestamp(lastModificationDate, query);

        setParamValues(playIds, effectiveOrgInfo, query);
        return query.list();
    }

    @SuppressWarnings("unchecked")
    @Override
    public List<Map<String, Object>> findRecommendationsAsMapByLaunchIds(List<String> launchIds, int offset, int max) {
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
                + ", lift AS " + PlaymakerConstants.Lift//
                + ", ratingModelId AS " + PlaymakerConstants.RatingModelId //
                + ", modelSummaryId AS " + PlaymakerConstants.ModelSummaryId //
                + " ) " //
                + "FROM %s " //
                + "WHERE deleted = :deleted " //
                + "AND launchId IN (:launchIds) " //
                + "ORDER BY lastUpdatedTimestamp";

        queryStr = String.format(queryStr, entityClz.getSimpleName());

        @SuppressWarnings("rawtypes")
        Query query = session.createQuery(queryStr);
        query.setMaxResults(max);
        query.setFirstResult(offset);
        query.setParameter("deleted", Boolean.FALSE);
        query.setParameterList("launchIds", launchIds);
        return query.list();
    }

    private String additionalWhereClause(List<String> playIds, Pair<String, String> effectiveOrgInfo, String queryStr) {
        if (!CollectionUtils.isEmpty(playIds)) {
            queryStr += "AND playId IN (:playIds) ";
        }
        if (effectiveOrgInfo != null) {
            queryStr += "AND destinationOrgId = :destinationOrgId ";
            queryStr += "AND destinationSysType = :destinationSysType ";
        } else {
            queryStr += "AND destinationOrgId is null ";
            queryStr += "AND destinationSysType is null ";
        }
        return queryStr;
    }

    private void setParamValues(List<String> playIds, Pair<String, String> effectiveOrgInfo, Query<?> query) {
        if (!CollectionUtils.isEmpty(playIds)) {
            query.setParameterList("playIds", playIds);
        }
        if (effectiveOrgInfo != null) {
            query.setParameter("destinationOrgId", effectiveOrgInfo.getLeft());
            query.setParameter("destinationSysType", effectiveOrgInfo.getRight());
        }
    }

    @Override
    public void deleteByRecommendationId(String recommendationId, boolean hardDelete) {
        Session session = getSessionFactory().getCurrentSession();

        Class<Recommendation> entityClz = getEntityClass();
        String updateQueryStr = "UPDATE %s SET deleted = :deleted " //
                + "WHERE recommendationId = :recommendationId ";

        updateQueryStr = String.format(updateQueryStr, entityClz.getSimpleName());
        Query<?> query = session.createQuery(updateQueryStr);
        query.setParameter("deleted", Boolean.TRUE);
        query.setParameter("recommendationId", recommendationId);
        query.executeUpdate();
    }

    @Override
    public int deleteInBulkByLaunchId(String launchId, boolean hardDelete, int maxUpdateRows) {
        Session session = getSessionFactory().getCurrentSession();

        Class<Recommendation> entityClz = getEntityClass();
        String selectQueryStr = "SELECT recommendationId " + "FROM %s " //
                + "WHERE launchId = :launchId " //
                + "AND deleted = :deleted ";

        selectQueryStr = String.format(selectQueryStr, entityClz.getSimpleName());

        Query<?> query = session.createQuery(selectQueryStr);
        query.setParameter("launchId", launchId);
        query.setParameter("deleted", Boolean.FALSE);
        query.setMaxResults(maxUpdateRows);
        List<?> recommendationIds = query.getResultList();

        return runBulkUpdate(session, entityClz, recommendationIds);
    }

    @SuppressWarnings("deprecation")
    @Override
    public int deleteInBulkByPlayId(String playId, Date cutoffDate, boolean hardDelete, int maxUpdateRows) {
        Session session = getSessionFactory().getCurrentSession();

        Class<Recommendation> entityClz = getEntityClass();
        String selectQueryStr = "SELECT recommendationId " //
                + "FROM %s " //
                + "WHERE playId = :playId " //
                + "AND deleted = :deleted ";
        if (cutoffDate != null) {
            selectQueryStr += "AND UNIX_TIMESTAMP(launchDate) <= :launchDate ";
        }

        selectQueryStr = String.format(selectQueryStr, entityClz.getSimpleName());

        Query<?> query = session.createQuery(selectQueryStr);
        query.setParameter("playId", playId);
        query.setParameter("deleted", Boolean.FALSE);
        if (cutoffDate != null) {
            query.setBigInteger("launchDate", new BigInteger((dateToUnixTimestamp(cutoffDate).toString())));
        }
        query.setMaxResults(maxUpdateRows);
        List<?> recommendationIds = query.getResultList();

        return runBulkUpdate(session, entityClz, recommendationIds);
    }

    @SuppressWarnings("deprecation")
    @Override
    public int deleteInBulkByCutoffDate(Date cutoffDate, boolean hardDelete, int maxUpdateRows) {
        if (cutoffDate == null) {
            return 0;
        }
        Session session = getSessionFactory().getCurrentSession();

        Class<Recommendation> entityClz = getEntityClass();
        String selectQueryStr = "SELECT recommendationId " //
                + "FROM %s " //
                + "WHERE UNIX_TIMESTAMP(launchDate) <= :launchDate " //
                + "AND deleted = :deleted ";

        selectQueryStr = String.format(selectQueryStr, entityClz.getSimpleName());

        Query<?> query = session.createQuery(selectQueryStr);
        query.setBigInteger("launchDate", new BigInteger((dateToUnixTimestamp(cutoffDate).toString())));
        query.setParameter("deleted", Boolean.FALSE);

        query.setMaxResults(maxUpdateRows);
        List<?> recommendationIds = query.getResultList();

        return runBulkUpdate(session, entityClz, recommendationIds);
    }

    private int runBulkUpdate(Session session, Class<Recommendation> entityClz, List<?> recommendationIds) {
        if (CollectionUtils.isEmpty(recommendationIds)) {
            return 0;
        }

        String updateQueryStr = "UPDATE %s " //
                + "SET deleted = :deleted " //
                + "WHERE recommendationId IN (:recommendationIds) ";

        updateQueryStr = String.format(updateQueryStr, entityClz.getSimpleName());

        Query<?> query = session.createQuery(updateQueryStr);
        query.setParameter("deleted", Boolean.TRUE);
        query.setParameterList("recommendationIds", recommendationIds);
        return query.executeUpdate();
    }

    @SuppressWarnings("unchecked")
    @Override
    public List<String> findAccountIdsByLaunchIds(List<String> launchIds, int offset, int max) {
        Session session = getSessionFactory().getCurrentSession();
        Class<Recommendation> entityClz = getEntityClass();
        String queryStr = "SELECT DISTINCT accountId FROM %s " //
                + "WHERE deleted = :deleted AND launchId in (:launchIds) ORDER BY accountId";

        queryStr = String.format(queryStr, entityClz.getSimpleName());

        Query<String> query = session.createQuery(queryStr);
        query.setMaxResults(max);
        query.setFirstResult(offset);
        query.setParameter("deleted", Boolean.FALSE);
        if (CollectionUtils.isNotEmpty(launchIds)) {
            query.setParameterList("launchIds", launchIds);
        } else {
            return null;
        }
        return query.list();
    }

    @SuppressWarnings("unchecked")
    @Override
    public List<String> findAccountIdsByLaunchIds(List<String> launchIds) {
        Session session = getSessionFactory().getCurrentSession();
        Class<Recommendation> entityClz = getEntityClass();
        String queryStr = "SELECT DISTINCT accountId FROM %s " //
                + "WHERE deleted = :deleted " + "AND launchId in (:launchIds) " + "order by accountId";

        queryStr = String.format(queryStr, entityClz.getSimpleName());

        Query<String> query = session.createQuery(queryStr);
        query.setParameter("deleted", Boolean.FALSE);
        if (CollectionUtils.isNotEmpty(launchIds)) {
            query.setParameterList("launchIds", launchIds);
        } else {
            return null;
        }
        return query.list();
    }

    @Override
    public int findAccountIdCountByLaunchIds(List<String> launchIds) {
        Session session = getSessionFactory().getCurrentSession();
        Class<Recommendation> entityClz = getEntityClass();
        String queryStr = "SELECT COUNT(DISTINCT accountId) FROM %s " //
                + "WHERE deleted = :deleted " + "AND launchId in (:launchIds) ";
        queryStr = String.format(queryStr, entityClz.getSimpleName());
        Query<?> query = session.createQuery(queryStr);
        query.setParameter("deleted", Boolean.FALSE);
        if (CollectionUtils.isNotEmpty(launchIds)) {
            query.setParameterList("launchIds", launchIds);
        } else {
            return 0;
        }
        return ((Long) query.uniqueResult()).intValue();
    }

    @SuppressWarnings("unchecked")
    @Override
    public List<Map<String, Object>> findContactsByLaunchIds(List<String> launchIds, List<String> accountIds) {
        log.info("contact requst with launchIds: " + launchIds.toString());
        if (CollectionUtils.isEmpty(launchIds)) {
            return null;
        }
        Session session = getSessionFactory().getCurrentSession();
        Class<Recommendation> entityClz = getEntityClass();
        String queryStr = "SELECT contacts FROM %s " //
                + "WHERE deleted = :deleted AND launchId in (:launchIds)";

        if (CollectionUtils.isNotEmpty(accountIds)) {
            queryStr += " AND accountId in (:accountIds)";
        }
        queryStr += " ORDER BY accountId";
        queryStr = String.format(queryStr, entityClz.getSimpleName());
        Query<String> query = session.createQuery(queryStr);
        query.setParameter("deleted", Boolean.FALSE);
        query.setParameterList("launchIds", launchIds);
        if (CollectionUtils.isNotEmpty(accountIds)) {
            query.setParameterList("accountIds", accountIds);
        }
        List<String> queryResult = query.list();
        List<Map<String, Object>> contacts = new ArrayList<Map<String, Object>>();
        queryResult.stream().forEach(contactStr -> {
            if (StringUtils.isNotBlank(contactStr)) {
                List<Map<String, Object>> contactsList = PlaymakerUtils.getExpandedContacts(contactStr, Object.class);
                contacts.addAll(contactsList);
            }
        });
        return contacts;
    }

    @SuppressWarnings("unchecked")
    @Override
    public int findContactsCountByLaunchIds(List<String> launchIds, List<String> accountIds) {
        List<Map<String, Object>> contacts = findContactsByLaunchIds(launchIds, accountIds);
        return contacts.size();
    }
}
