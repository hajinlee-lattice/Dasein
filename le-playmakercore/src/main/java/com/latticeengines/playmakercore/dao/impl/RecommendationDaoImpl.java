package com.latticeengines.playmakercore.dao.impl;

import java.math.BigInteger;
import java.util.Date;
import java.util.List;
import java.util.Map;

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
import com.latticeengines.domain.exposed.cdl.CDLConstants;
import com.latticeengines.domain.exposed.playmaker.PlaymakerConstants;
import com.latticeengines.domain.exposed.playmakercore.Recommendation;
import com.latticeengines.playmakercore.dao.RecommendationDao;

@Component("recommendationDao")
public class RecommendationDaoImpl extends BaseDaoWithAssignedSessionFactoryImpl<Recommendation>
        implements RecommendationDao {

    private static final Logger log = LoggerFactory.getLogger(RecommendationDaoImpl.class);

    @Override
    protected Class<Recommendation> getEntityClass() {
        return Recommendation.class;
    }

    @SuppressWarnings("unchecked")
    @Override
    public Recommendation findByRecommendationId(String recommendationId) {
        Session session = getSessionFactory().getCurrentSession();
        Class<Recommendation> entityClz = getEntityClass();
        String queryStr = String.format("FROM %s WHERE recommendationId = :recommendationId",
                entityClz.getSimpleName());
        Query query = session.createQuery(queryStr);
        query.setString("recommendationId", recommendationId);
        return (Recommendation) query.uniqueResult();

        //
        // CriteriaBuilder builder = session.getCriteriaBuilder();
        // CriteriaQuery<Recommendation> query =
        // builder.createQuery(Recommendation.class);
        // Root<Recommendation> root = query.from(Recommendation.class);
        // query.select(root).where(builder.equal(root.get("recommendationId"),
        // recommendationId));
        //
        // return session.createQuery(query).uniqueResult();
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
            String syncDestination, List<String> playIds, Map<String, String> orgInfo) {
        Session session = getSessionFactory().getCurrentSession();

        Pair<String, String> effectiveOrgInfo = getEffectiveOrgInfo(orgInfo);

        Class<Recommendation> entityClz = getEntityClass();
        String queryStr = "FROM %s WHERE synchronizationDestination = :syncDestination " //
                + "AND UNIX_TIMESTAMP(lastUpdatedTimestamp) >= :lastUpdatedTimestamp ";

        queryStr = additionalWhereClause(playIds, effectiveOrgInfo, queryStr);

        queryStr = String.format(queryStr, entityClz.getSimpleName());
        Query query = session.createQuery(queryStr);
        query.setMaxResults(max);
        query.setFirstResult(offset);
        query.setString("syncDestination", syncDestination);

        updateQueryWithLastUpdatedTimestamp(lastModificationDate, query);

        setParamValues(playIds, effectiveOrgInfo, query);

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
    public int findRecommendationCount(Date lastModificationDate, String syncDestination, List<String> playIds,
            Map<String, String> orgInfo) {
        Session session = getSessionFactory().getCurrentSession();

        Pair<String, String> effectiveOrgInfo = getEffectiveOrgInfo(orgInfo);

        Class<Recommendation> entityClz = getEntityClass();
        String queryStr = "SELECT count(*) FROM %s WHERE synchronizationDestination = :syncDestination " //
                + "AND UNIX_TIMESTAMP(lastUpdatedTimestamp) >= :lastUpdatedTimestamp ";

        queryStr = additionalWhereClause(playIds, effectiveOrgInfo, queryStr);

        queryStr += " ORDER BY lastUpdatedTimestamp ";
        queryStr = String.format(queryStr, entityClz.getSimpleName());
        Query query = session.createQuery(queryStr);
        query.setString("syncDestination", syncDestination);

        updateQueryWithLastUpdatedTimestamp(lastModificationDate, query);

        setParamValues(playIds, effectiveOrgInfo, query);
        return ((Long) query.uniqueResult()).intValue();
    }

    @SuppressWarnings("unchecked")
    @Override
    public List<Map<String, Object>> findRecommendationsAsMap(Date lastModificationDate, int offset, int max,
            String syncDestination, List<String> playIds, Map<String, String> orgInfo) {
        Session session = getSessionFactory().getCurrentSession();

        Pair<String, String> effectiveOrgInfo = getEffectiveOrgInfo(orgInfo);

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

        queryStr = additionalWhereClause(playIds, effectiveOrgInfo, queryStr);

        queryStr += " ORDER BY lastUpdatedTimestamp ";
        queryStr = String.format(queryStr, entityClz.getSimpleName());

        Query query = session.createQuery(queryStr);
        query.setMaxResults(max);
        query.setFirstResult(offset);
        query.setString("syncDestination", syncDestination);

        updateQueryWithLastUpdatedTimestamp(lastModificationDate, query);

        setParamValues(playIds, effectiveOrgInfo, query);
        return query.list();
    }

    private Pair<String, String> getEffectiveOrgInfo(Map<String, String> orgInfo) {
        Pair<String, String> effectiveOrgInfo = null;

        if (MapUtils.isNotEmpty(orgInfo)) {
            log.info(String.format("Org info for this request: %s = %s, %s = %s. ", CDLConstants.ORG_ID,
                    orgInfo.get(CDLConstants.ORG_ID), CDLConstants.EXTERNAL_SYSTEM_TYPE,
                    orgInfo.get(CDLConstants.EXTERNAL_SYSTEM_TYPE)));

            if (StringUtils.isNotBlank(orgInfo.get(CDLConstants.ORG_ID))
                    && StringUtils.isNotBlank(orgInfo.get(CDLConstants.EXTERNAL_SYSTEM_TYPE))) {
                effectiveOrgInfo = new ImmutablePair<String, String>(orgInfo.get(CDLConstants.ORG_ID).trim(),
                        orgInfo.get(CDLConstants.EXTERNAL_SYSTEM_TYPE).trim());

                log.info(String.format("Effective org info: %s = %s, %s = %s", //
                        CDLConstants.ORG_ID, effectiveOrgInfo.getLeft(), //
                        CDLConstants.EXTERNAL_SYSTEM_TYPE, effectiveOrgInfo.getRight()));
            }
        }
        return effectiveOrgInfo;
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

    private void setParamValues(List<String> playIds, Pair<String, String> effectiveOrgInfo, Query query) {
        if (!CollectionUtils.isEmpty(playIds)) {
            query.setParameterList("playIds", playIds);
        }
        if (effectiveOrgInfo != null) {
            query.setParameter("destinationOrgId", effectiveOrgInfo.getLeft());
            query.setParameter("destinationSysType", effectiveOrgInfo.getRight());
        }
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
