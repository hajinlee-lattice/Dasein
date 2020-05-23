package com.latticeengines.playmakercore.dao.impl;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import javax.persistence.Table;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.hibernate.Session;
import org.hibernate.query.Query;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.db.exposed.dao.impl.BaseDaoWithAssignedSessionFactoryImpl;
import com.latticeengines.domain.exposed.playmaker.PlaymakerConstants;
import com.latticeengines.domain.exposed.playmaker.PlaymakerUtils;
import com.latticeengines.domain.exposed.playmakercore.Recommendation;
import com.latticeengines.domain.exposed.pls.LookupIdMapUtils;
import com.latticeengines.playmakercore.dao.RecommendationDao;

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
                // + "AND UNIX_TIMESTAMP(lastUpdatedTimestamp) >=
                // :lastUpdatedTimestamp " //
                + "AND deleted = :deleted ";
        queryStr = String.format(queryStr, entityClz.getSimpleName());
        Query<?> query = session.createQuery(queryStr);
        query.setParameterList("launchIds", launchIds);
        query.setParameter("deleted", Boolean.FALSE);
        // query.setParameter("lastUpdatedTimestamp", start);
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
                + "AND UNIX_TIMESTAMP( lastUpdatedTimestamp ) >= :lastUpdatedTimestamp " //
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
        return lastModificationDate.getTime() / 1000;
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
                + "AND UNIX_TIMESTAMP( lastUpdatedTimestamp ) >= :lastUpdatedTimestamp " //
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
                + "AND UNIX_TIMESTAMP( lastUpdatedTimestamp ) >= :lastUpdatedTimestamp " //
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

    @Override
    public List<Map<String, Object>> findRecommendationsAsMapByLaunchIds(List<String> launchIds, long start, int offset, int max) {
        Session session = getSessionFactory().getCurrentSession();
        Class<Recommendation> entityClz = getEntityClass();
        String recommendationTable = entityClz.getAnnotation(Table.class).name();
        String queryStr = "SELECT "
                + " EXTERNAL_ID, ACCOUNT_ID, LE_ACCOUNT_EXTERNAL_ID, PLAY_ID, LAUNCH_ID, DESCRIPTION"
                + ", UNIX_TIMESTAMP(LAUNCH_DATE), UNIX_TIMESTAMP(LAST_UPDATED_TIMESTAMP), MONETARY_VALUE"
                + ", LIKELIHOOD, COMPANY_NAME, SFDC_ACCOUNT_ID, PRIORITY_ID, PRIORITY_DISPLAY_NAME"
                + ", MONETARY_VALUE_ISO4217_ID, CONTACTS, LIFT, RATING_MODEL_ID, MODEL_SUMMARY_ID"
                + " FROM %s USE INDEX(REC_LAUNCH_ID) " //
                + " WHERE DELETED = :deleted " //
                + " AND LAUNCH_ID IN (:launchIds) ";
        if (launchIds.size() == 1) {
            queryStr += "ORDER BY PID ";
        } else {
            queryStr += "ORDER BY LAST_UPDATED_TIMESTAMP, PID ";
        }
        queryStr = String.format(queryStr, recommendationTable);
        @SuppressWarnings("rawtypes")
        Query query = session.createSQLQuery(queryStr);
        query.setMaxResults(max);
        query.setFirstResult(offset);
        query.setParameter("deleted", Boolean.FALSE);
        query.setParameterList("launchIds", launchIds);
        return extractResultToMap(query.list());
    }

    private List<Map<String, Object>> extractResultToMap(List<Object[]> objects) {
        if (CollectionUtils.isEmpty(objects)) {
            return new ArrayList<>();
        }
        return objects.stream().map(object -> {
            Map<String, Object> map = new HashMap<>();
            map.put(PlaymakerConstants.ID, object[0]);
            map.put(PlaymakerConstants.AccountID, object[1]);
            map.put(PlaymakerConstants.LEAccountExternalID, object[2]);
            map.put(PlaymakerConstants.PlayID, object[3]);
            map.put(PlaymakerConstants.LaunchID, object[4]);
            map.put(PlaymakerConstants.Description, object[5]);
            map.put(PlaymakerConstants.LaunchDate, ((BigInteger)object[6]).longValue());
            map.put(PlaymakerConstants.LastModificationDate, ((BigInteger)object[7]).longValue());
            map.put(PlaymakerConstants.MonetaryValue, object[8]);
            map.put(PlaymakerConstants.Likelihood, object[9]);
            map.put(PlaymakerConstants.CompanyName, object[10]);
            map.put(PlaymakerConstants.SfdcAccountID, object[11]);
            map.put(PlaymakerConstants.PriorityID, object[12]);
            map.put(PlaymakerConstants.PriorityDisplayName, object[13]);
            map.put(PlaymakerConstants.MonetaryValueIso4217ID, object[14]);
            map.put(PlaymakerConstants.Contacts, object[15]);
            map.put(PlaymakerConstants.Lift, object[16]);
            map.put(PlaymakerConstants.RatingModelId, object[17]);
            map.put(PlaymakerConstants.ModelSummaryId, object[18]);
            return map;
        }).collect(Collectors.toList());
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
        String softDeleteQueryStr = "UPDATE " + getEntityClass().getSimpleName() + " " //
                + "SET deleted = :deleted " //
                + "WHERE launchId = :launchId AND deleted = :notDeleted ";

        Query<?> query = session.createQuery(softDeleteQueryStr);
        query.setParameter("launchId", launchId);
        query.setParameter("deleted", Boolean.TRUE);
        query.setParameter("notDeleted", Boolean.FALSE);
        query.setMaxResults(maxUpdateRows);

        return query.executeUpdate();
    }

    @SuppressWarnings("deprecation")
    @Override
    public int deleteInBulkByPlayId(String playId, Date cutoffDate, boolean hardDelete, int maxUpdateRows) {
        Session session = getSessionFactory().getCurrentSession();
        String softDeleteQueryStr = "UPDATE " + getEntityClass().getSimpleName() + " " //
                + "SET deleted = :deleted " //
                + "WHERE playId = :playId AND deleted = :notDeleted ";
        if (cutoffDate != null) {
            softDeleteQueryStr += "AND UNIX_TIMESTAMP(launchDate) <= :launchDate ";
        }
        Query<?> query = session.createQuery(softDeleteQueryStr);
        query.setParameter("playId", playId);
        query.setParameter("deleted", Boolean.TRUE);
        query.setParameter("notDeleted", Boolean.FALSE);
        query.setMaxResults(maxUpdateRows);

        if (cutoffDate != null) {
            query.setBigInteger("launchDate", new BigInteger((dateToUnixTimestamp(cutoffDate).toString())));
        }

        return query.executeUpdate();
    }

    @SuppressWarnings("deprecation")
    @Override
    public int deleteInBulkByCutoffDate(Date cutoffDate, boolean hardDelete, int maxUpdateRows) {
        if (cutoffDate == null) {
            return 0;
        }
        Session session = getSessionFactory().getCurrentSession();
        String softDeleteQueryStr = "UPDATE " + getEntityClass().getSimpleName() + " " //
                + "SET deleted = :deleted " //
                + "WHERE UNIX_TIMESTAMP(launchDate) <= :launchDate AND deleted = :notDeleted ";

        Query<?> query = session.createQuery(softDeleteQueryStr);
        query.setParameter("deleted", Boolean.TRUE);
        query.setParameter("notDeleted", Boolean.FALSE);
        query.setBigInteger("launchDate", new BigInteger((dateToUnixTimestamp(cutoffDate).toString())));
        query.setMaxResults(maxUpdateRows);

        return query.executeUpdate();
    }

    @SuppressWarnings("unchecked")
    @Override
    public List<Map<String, Object>> findAccountIdsByLaunchIds(List<String> launchIds, long start, int offset,
            int max) {
        Session session = getSessionFactory().getCurrentSession();
        Class<Recommendation> entityClz = getEntityClass();

        String queryStr = "SELECT " + "DISTINCT( accountId ), "//
                + "UNIX_TIMESTAMP( lastUpdatedTimestamp ) as lastUpdatedUnixTimestamp " //
                + "FROM %s " //
                + "WHERE deleted = :deleted " //
                + "AND launchId IN (:launchIds) " //
                + "AND UNIX_TIMESTAMP( lastUpdatedTimestamp ) >= :lastUpdatedTimestamp " //
                + "ORDER BY lastUpdatedUnixTimestamp, accountId";

        queryStr = String.format(queryStr, entityClz.getSimpleName());

        @SuppressWarnings("rawtypes")
        Query<Object[]> query = session.createQuery(queryStr);
        query.setMaxResults(max);
        query.setFirstResult(offset);
        query.setParameter("deleted", Boolean.FALSE);
        query.setParameter("lastUpdatedTimestamp", start);
        if (CollectionUtils.isNotEmpty(launchIds)) {
            query.setParameterList("launchIds", launchIds);
        } else {
            return null;
        }

        List<Map<String, Object>> results = new ArrayList<>();
        List<Object[]> queryResult = query.list();

        if (CollectionUtils.isNotEmpty(queryResult)) {
            queryResult.stream().forEach(result -> {
                String accountId = (String) result[0];
                long lastModifyTs = (long) result[1];
                Map<String, Object> obj = new HashMap<String, Object>();
                obj.put(PlaymakerConstants.AccountID, accountId);
                obj.put(PlaymakerConstants.LastModificationDate, lastModifyTs);
                results.add(obj);
            });
        }
        return results;
    }

    @Override
    public int findAccountIdCountByLaunchIds(List<String> launchIds, long start) {
        Session session = getSessionFactory().getCurrentSession();
        Class<Recommendation> entityClz = getEntityClass();
        String queryStr = "SELECT COUNT(DISTINCT accountId) FROM %s " //
                + "WHERE deleted = :deleted " + "AND UNIX_TIMESTAMP( lastUpdatedTimestamp ) >= :lastUpdatedTimestamp " //
                + "AND launchId in (:launchIds) ";
        queryStr = String.format(queryStr, entityClz.getSimpleName());
        Query<?> query = session.createQuery(queryStr);
        query.setParameter("lastUpdatedTimestamp", start);
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
    public List<Map<String, Object>> findContactsByLaunchIds(List<String> launchIds, long start, int offset,
            int maximum, List<String> accountIds) {
        log.info("contact requst with launchIds: " + launchIds.toString());
        if (CollectionUtils.isEmpty(launchIds)) {
            return Collections.emptyList();
        }
        Session session = getSessionFactory().getCurrentSession();
        Class<Recommendation> entityClz = getEntityClass();
        String queryStr = "SELECT accountId,contacts FROM %s " //
                + "WHERE deleted = :deleted AND launchId in (:launchIds) "
                + "AND UNIX_TIMESTAMP( lastUpdatedTimestamp ) >= :lastUpdatedTimestamp "; //

        if (CollectionUtils.isNotEmpty(accountIds)) {
            queryStr += " AND accountId in (:accountIds)";
        }
        queryStr += " ORDER BY accountId";
        queryStr = String.format(queryStr, entityClz.getSimpleName());
        Query<Object[]> query = session.createQuery(queryStr);
        query.setParameter("deleted", Boolean.FALSE);
        query.setMaxResults(maximum);
        query.setFirstResult(offset);
        query.setParameterList("launchIds", launchIds);
        query.setParameter("lastUpdatedTimestamp", start);
        if (CollectionUtils.isNotEmpty(accountIds)) {
            query.setParameterList("accountIds", accountIds);
        }
        List<Object[]> queryResult = (List<Object[]>) query.list();
        List<Map<String, Object>> contacts = new ArrayList<Map<String, Object>>();
        String accountIdName = PlaymakerConstants.AccountID + PlaymakerConstants.V2;
        queryResult.stream().forEach(result -> {
            String accountId = (String) result[0];
            String contactStr = (String) result[1];
            if (StringUtils.isNotBlank(contactStr)) {
                List<Map<String, Object>> contactsList = PlaymakerUtils.getExpandedContacts(contactStr, Object.class);
                contactsList.stream().forEach(c -> {
                    c.put(accountIdName, accountId);
                    contacts.add(c);
                });
            }
        });
        return contacts;
    }
}
