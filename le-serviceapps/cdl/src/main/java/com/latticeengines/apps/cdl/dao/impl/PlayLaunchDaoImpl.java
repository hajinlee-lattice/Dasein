package com.latticeengines.apps.cdl.dao.impl;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.hibernate.Query;
import org.hibernate.Session;
import org.springframework.stereotype.Component;

import com.latticeengines.apps.cdl.dao.PlayLaunchDao;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.NamingUtils;
import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.pls.LaunchState;
import com.latticeengines.domain.exposed.pls.LaunchSummary;
import com.latticeengines.domain.exposed.pls.Play;
import com.latticeengines.domain.exposed.pls.PlayLaunch;
import com.latticeengines.domain.exposed.pls.PlayLaunchDashboard.Stats;

@SuppressWarnings("deprecation")
@Component("playLaunchDao")
public class PlayLaunchDaoImpl extends BaseDaoImpl<PlayLaunch> implements PlayLaunchDao {

    private static final String CREATED_COL = "created";
    private static final String DEST_ORG_ID = "destinationOrgId";
    private static final String DEST_SYS_TYPE = "destinationSysType";

    @Override
    protected Class<PlayLaunch> getEntityClass() {
        return PlayLaunch.class;
    }

    @SuppressWarnings({ "rawtypes" })
    @Override
    public PlayLaunch findByLaunchId(String launchId) {
        if (StringUtils.isBlank(launchId)) {
            return null;
        }

        Session session = getSessionFactory().getCurrentSession();
        Class<PlayLaunch> entityClz = getEntityClass();
        String queryStr = String.format(" FROM %s " //
                + " WHERE launch_id = :launchId ", //
                entityClz.getSimpleName());
        Query query = session.createQuery(queryStr);
        query.setParameter("launchId", launchId);
        List list = query.list();
        if (list.size() == 0) {
            return null;
        }
        return (PlayLaunch) list.get(0);
    }

    @SuppressWarnings({ "rawtypes" })
    @Override
    public PlayLaunch findByPlayAndTimestamp(Long playId, Date created) {
        if (playId == null) {
            return null;
        }

        Session session = getSessionFactory().getCurrentSession();
        Class<PlayLaunch> entityClz = getEntityClass();
        String queryStr = String.format(" FROM %s "//
                + " WHERE fk_play_id = :playId "//
                + " AND created = :created ", //
                entityClz.getSimpleName());
        Query query = session.createQuery(queryStr);
        query.setParameter("playId", playId);
        query.setParameter("created", created);
        List list = query.list();
        if (list.size() == 0) {
            return null;
        }
        return (PlayLaunch) list.get(0);
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Override
    public PlayLaunch findLatestByPlayAndSysOrg(Long playId, String orgId) {
        if (playId == null) {
            return null;
        }

        Session session = getSessionFactory().getCurrentSession();
        Class<PlayLaunch> entityClz = getEntityClass();
        String queryStr = String.format(" FROM %s "//
                + " WHERE fk_play_id = :playId "//
                + " AND destination_org_id = :orgId ", //
                entityClz.getSimpleName());

        queryStr += " ORDER BY created DESC ";

        Query query = session.createQuery(queryStr);
        query.setParameter("playId", playId);
        query.setParameter("orgId", orgId.trim());
        List list = query.list();
        if (list.size() == 0) {
            return null;
        }
        return (PlayLaunch) list.get(0);
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Override
    public List<PlayLaunch> findByPlayId(Long playId, List<LaunchState> states) {
        if (playId == null) {
            return null;
        }

        Session session = getSessionFactory().getCurrentSession();
        Class<PlayLaunch> entityClz = getEntityClass();

        String queryStr = String.format(" FROM %s "//
                + " WHERE fk_play_id = :playId ", //
                entityClz.getSimpleName());

        if (CollectionUtils.isNotEmpty(states)) {
            queryStr += " AND state IN ( :states ) ";
        }

        queryStr += " ORDER BY created DESC ";

        Query query = session.createQuery(queryStr);
        query.setParameter("playId", playId);

        if (CollectionUtils.isNotEmpty(states)) {
            List<String> statesNameList = states.stream()//
                    .map(LaunchState::name)//
                    .collect(Collectors.toList());

            query.setParameterList("states", statesNameList);
        }
        return query.list();
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Override
    public List<PlayLaunch> findByState(LaunchState state) {
        if (state == null) {
            throw new RuntimeException("Valid launch state is needed");
        }

        Session session = getSessionFactory().getCurrentSession();
        String queryStr = String.format(" FROM %s " //
                + " WHERE state = :state " //
                + " ORDER BY created DESC ", //
                getEntityClass().getSimpleName());
        Query query = session.createQuery(queryStr);
        query.setParameter("state", state.name());
        return query.list();
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Override
    public List<PlayLaunch> getByStateAcrossTenants(LaunchState state, Long max) {
        if (state == null) {
            throw new RuntimeException("Valid launch state is needed");
        }

        Session session = getSessionFactory().getCurrentSession();
        String queryStr = String.format(" FROM %s " //
                + " WHERE state = :state and deleted = :deleted" //
                + " ORDER BY created DESC ", //
                getEntityClass().getSimpleName());
        Query query = session.createQuery(queryStr);
        query.setParameter("state", state.name());
        query.setParameter("deleted", Boolean.FALSE);
        if (max != null) {
            query.setMaxResults(max.intValue());
        }
        return query.list();
    }

    @Override
    public PlayLaunch findLatestByPlayId(Long playId, List<LaunchState> states) {
        List<PlayLaunch> playLaunchList = findByPlayId(playId, states);
        if (playLaunchList != null && playLaunchList.size() > 0) {
            return playLaunchList.get(0);
        }
        return null;
    }

    @Override
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public List<LaunchSummary> findByPlayStatesAndPagination(Long playId, List<LaunchState> states, Long startTimestamp,
            Long offset, Long max, String sortby, boolean descending, Long endTimestamp, String orgId,
            String externalSysType) {
        Session session = getSessionFactory().getCurrentSession();
        Class<PlayLaunch> entityClz = getEntityClass();

        String queryStr = "SELECT new com.latticeengines.domain.exposed.pls.LaunchSummary (pl)";

        Query query = createQueryForDashboard(playId, states, startTimestamp, offset, max, sortby, descending,
                endTimestamp, session, entityClz, queryStr, true, orgId, externalSysType);

        return query.list();
    }

    @SuppressWarnings({ "rawtypes" })
    @Override
    public Long findCountByPlayStatesAndTimestamps(Long playId, List<LaunchState> states, Long startTimestamp,
            Long endTimestamp, String orgId, String externalSysType) {
        Session session = getSessionFactory().getCurrentSession();
        Class<PlayLaunch> entityClz = getEntityClass();

        String queryStr = "SELECT count(*) ";
        Query query = createQueryForDashboard(playId, states, startTimestamp, null, null, null, true, endTimestamp,
                session, entityClz, queryStr, false, orgId, externalSysType);

        return Long.parseLong(query.uniqueResult().toString());
    }

    @Override
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public List<Play> findDashboardPlaysWithLaunches(Long playId, List<LaunchState> states, Long startTimestamp,
            Long endTimestamp, String orgId, String externalSysType) {
        Session session = getSessionFactory().getCurrentSession();
        Class<PlayLaunch> entityClz = getEntityClass();

        String queryStr = "FROM Play WHERE pid IN ( SELECT distinct play ";
        Query query = createQueryForDashboard(playId, states, startTimestamp, null, null, null, true, endTimestamp,
                session, entityClz, queryStr, ")", false, orgId, externalSysType);
        return query.list();
    }

    @Override
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public List<Pair<String, String>> findDashboardOrgIdWithLaunches(Long playId, List<LaunchState> states,
            Long startTimestamp, Long endTimestamp, String orgId, String externalSysType) {
        Session session = getSessionFactory().getCurrentSession();
        Class<PlayLaunch> entityClz = getEntityClass();

        String queryStr = "SELECT distinct " + DEST_ORG_ID + ", " + DEST_SYS_TYPE + " ";
        Query query = createQueryForDashboard(playId, states, startTimestamp, null, null, null, true, endTimestamp,
                session, entityClz, queryStr, null, false, orgId, externalSysType);
        List<Object> queryResult = query.list();

        List<Pair<String, String>> result = new ArrayList<>();
        if (CollectionUtils.isNotEmpty(queryResult)) {
            queryResult.forEach(org -> {
                List<Object> obj = JsonUtils.convertList(JsonUtils.deserialize(JsonUtils.serialize(org), List.class),
                        Object.class);
                Object orgIdObj = obj.get(0);
                String extractedOrgId = orgIdObj == null ? null : orgIdObj.toString();
                Object sysTypeObj = obj.get(1);
                String extractedSysType = sysTypeObj == null ? null : sysTypeObj.toString();
                if (StringUtils.isNotBlank(extractedOrgId) && StringUtils.isNotBlank(extractedSysType)) {
                    result.add(new ImmutablePair<>(extractedOrgId, extractedSysType));
                }
            });
        }
        return result;
    }

    @Override
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public Stats findTotalCountByPlayStatesAndTimestamps(Long playId, List<LaunchState> states, Long startTimestamp,
            Long endTimestamp, String orgId, String externalSysType) {
        Session session = getSessionFactory().getCurrentSession();
        Class<PlayLaunch> entityClz = getEntityClass();
        String totalAccountsSelected = "totalAccountsSelected";
        String totalAccountsLaunched = "totalAccountsLaunched";
        String totalAccountsSuppressed = "totalAccountsSuppressed";
        String totalAccountsErrored = "totalAccountsErrored";
        String totalContactsLaunched = "totalContactsLaunched";
        String totalContactsSelected = "totalContactsSelected";
        String totalContactsSuppressed = "totalContactsSuppressed";
        String totalContactsErrored = "totalContactsErrored";

        String queryStr = "SELECT new map " + "( " //
                + " SUM(COALESCE(accountsSelected)) AS " + totalAccountsSelected + ", " //
                + " SUM(COALESCE(accountsLaunched)) AS " + totalAccountsLaunched + ", " //
                + " SUM(COALESCE(accountsSuppressed)) AS " + totalAccountsSuppressed + ", " //
                + " SUM(COALESCE(accountsErrored)) AS " + totalAccountsErrored + ", " //
                + " SUM(COALESCE(contactsLaunched)) AS " + totalContactsLaunched + ", " //
                + " SUM(COALESCE(contactsSelected)) AS " + totalContactsSelected + ", " //
                + " SUM(COALESCE(contactsSuppressed)) AS " + totalContactsSuppressed + ", " //
                + " SUM(COALESCE(contactsErrored)) AS " + totalContactsErrored + " " //
                + ") ";

        Query query = createQueryForDashboard(playId, states, startTimestamp, null, null, null, true, endTimestamp,
                session, entityClz, queryStr, false, orgId, externalSysType);

        List<Map<String, Object>> queryResult = query.list();
        Stats totalCounts = new Stats();
        Map<String, Object> res = queryResult.get(0);
        totalCounts.setSelectedTargets(getVal(res, totalAccountsSelected));
        totalCounts.setSelectedContacts(getVal(res, totalContactsSelected));
        totalCounts.setRecommendationsLaunched(getVal(res, totalAccountsLaunched));
        totalCounts.setAccountsSuppressed(getVal(res, totalAccountsSuppressed));
        totalCounts.setAccountErrors(getVal(res, totalAccountsErrored));
        totalCounts.setContactsSuppressed(getVal(res, totalContactsSuppressed));
        totalCounts.setContactErrors(getVal(res, totalContactsErrored));
        totalCounts.setContactsWithinRecommendations(getVal(res, totalContactsLaunched));

        return totalCounts;
    }

    private Long getVal(Map<String, Object> resMap, String key) {
        Object val = resMap.get(key);
        return val == null ? 0L : (Long) val;
    }

    @SuppressWarnings({ "rawtypes" })
    private Query createQueryForDashboard(Long playId, List<LaunchState> states, Long startTimestamp, Long offset,
            Long max, String sortby, boolean descending, Long endTimestamp, Session session,
            Class<PlayLaunch> entityClz, String queryStr, boolean sortNeeded, String orgId, String externalSysType) {
        return createQueryForDashboard(playId, states, startTimestamp, offset, max, sortby, descending, endTimestamp,
                session, entityClz, queryStr, null, sortNeeded, orgId, externalSysType);
    }

    @SuppressWarnings({ "rawtypes" })
    private Query createQueryForDashboard(Long playId, List<LaunchState> states, Long startTimestamp, Long offset,
            Long max, String sortby, boolean descending, Long endTimestamp, Session session,
            Class<PlayLaunch> entityClz, String queryStr, String closingQueryStr, boolean sortNeeded, String orgId,
            String externalSysType) {

        queryStr += String.format(" FROM %s pl"//
                + " WHERE deleted = :deleted AND pl.created >= :startTimestamp ", //
                entityClz.getSimpleName());

        if (endTimestamp != null) {
            queryStr += " AND pl.created <= :endTimestamp  ";
        }

        if (playId != null) {
            queryStr += " AND fk_play_id = :playId ";
        }

        if (CollectionUtils.isNotEmpty(states)) {
            queryStr += " AND state IN ( :states ) ";
        }

        boolean orgIdFilterNeeded = false;
        if (StringUtils.isNotBlank(orgId) && StringUtils.isNotBlank(externalSysType)) {
            orgIdFilterNeeded = true;
            queryStr += " AND pl." + DEST_ORG_ID + " = :" + DEST_ORG_ID + " ";
            queryStr += " AND pl." + DEST_SYS_TYPE + " = :" + DEST_SYS_TYPE + " ";
        }

        if (sortNeeded) {
            String sortDirection = descending ? "DESC" : "ASC";

            if (StringUtils.isBlank(sortby) || sortby.trim().equalsIgnoreCase(CREATED_COL)) {
                queryStr += String.format(" ORDER BY pl.%s %s ", CREATED_COL, sortDirection);
            } else {
                queryStr += String.format(" ORDER BY pl.%s %s, pl.%s %s ", sortby, sortDirection, CREATED_COL,
                        sortDirection);
            }
        }

        if (StringUtils.isNotBlank(closingQueryStr)) {
            queryStr += closingQueryStr;
        }

        Query query = session.createQuery(queryStr);

        query.setParameter("deleted", Boolean.FALSE);

        if (offset != null) {
            query.setFirstResult(offset.intValue());
        }
        if (max != null) {
            query.setMaxResults(max.intValue());
        }

        query.setTimestamp("startTimestamp", //
                toTimestamp(startTimestamp));

        if (endTimestamp != null) {
            query.setTimestamp("endTimestamp", //
                    toTimestamp(endTimestamp));
        }

        if (playId != null) {
            query.setLong("playId", playId);
        }

        if (orgIdFilterNeeded) {
            query.setParameter(DEST_ORG_ID, orgId.trim());
            query.setParameter(DEST_SYS_TYPE, externalSysType.trim());
        }

        if (CollectionUtils.isNotEmpty(states)) {
            List<String> statesNameList = states.stream()//
                    .map(LaunchState::name)//
                    .collect(Collectors.toList());

            query.setParameterList("states", statesNameList);
        }

        return query;
    }

    private Date toTimestamp(Long timestamp) {
        if (timestamp == null || timestamp < 0) {
            timestamp = 0L;
        }

        return new Date(timestamp);
    }

    @Override
    public void create(PlayLaunch entity) {
        if (entity.getLaunchId() == null) {
            entity.setLaunchId(NamingUtils.uuid("pl"));
        }
        super.create(entity);
    }
}
