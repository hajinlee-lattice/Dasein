package com.latticeengines.pls.dao.impl;

import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.hibernate.Query;
import org.hibernate.Session;
import org.springframework.stereotype.Component;

import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.pls.LaunchState;
import com.latticeengines.domain.exposed.pls.Play;
import com.latticeengines.domain.exposed.pls.PlayLaunch;
import com.latticeengines.domain.exposed.pls.PlayLaunchDashboard.Stats;
import com.latticeengines.pls.dao.PlayLaunchDao;

@Component("playLaunchDao")
public class PlayLaunchDaoImpl extends BaseDaoImpl<PlayLaunch> implements PlayLaunchDao {

    private static final String CREATED_COL = "created";

    @Override
    protected Class<PlayLaunch> getEntityClass() {
        return PlayLaunch.class;
    }
    @SuppressWarnings("rawtypes")
    @Override
    public PlayLaunch findByLaunchId(String launchId) {
        if (StringUtils.isBlank(launchId)) {
            return null;
        }

        Session session = getSessionFactory().getCurrentSession();
        Class<PlayLaunch> entityClz = getEntityClass();
        String queryStr = String.format(
                " FROM %s " //
                        + " WHERE launch_id = :launchId ", //
                entityClz.getSimpleName());
        Query query = session.createQuery(queryStr);
        query.setString("launchId", launchId);
        List list = query.list();
        if (list.size() == 0) {
            return null;
        }
        return (PlayLaunch) list.get(0);
    }
	
    @SuppressWarnings("rawtypes")
    @Override
    public PlayLaunch findByPlayAndTimestamp(Long playId, Date created) {
        if (playId == null) {
            return null;
        }

        Session session = getSessionFactory().getCurrentSession();
        Class<PlayLaunch> entityClz = getEntityClass();
        String queryStr = String.format(
                " FROM %s "//
                        + " WHERE fk_play_id = :playId "//
                        + " AND created = :created ", //
                entityClz.getSimpleName());
        Query query = session.createQuery(queryStr);
        query.setLong("fk_play_id", playId);
        query.setDate("created", created);
        List list = query.list();
        if (list.size() == 0) {
            return null;
        }
        return (PlayLaunch) list.get(0);
    }
     
    @SuppressWarnings("unchecked")
    @Override
    public List<PlayLaunch> findByPlayId(Long playId, List<LaunchState> states) {
        if (playId == null) {
            return null;
        }

        Session session = getSessionFactory().getCurrentSession();
        Class<PlayLaunch> entityClz = getEntityClass();

        String queryStr = String.format(
                " FROM %s "//
                        + " WHERE fk_play_id = :playId ", //
                entityClz.getSimpleName());

        if (CollectionUtils.isNotEmpty(states)) {
            queryStr += " AND state IN ( :states ) ";
        }

        queryStr += " ORDER BY created DESC ";

        Query query = session.createQuery(queryStr);
        query.setLong("playId", playId);

        if (CollectionUtils.isNotEmpty(states)) {
            List<String> statesNameList = states.stream()//
                    .map(LaunchState::name)//
                    .collect(Collectors.toList());

            query.setParameterList("states", statesNameList);
        }
        return query.list();
    }

    @SuppressWarnings("unchecked")
    @Override
    public List<PlayLaunch> findByState(LaunchState state) {
        if (state == null) {
            throw new RuntimeException("Valid launch state is needed");
        }

        Session session = getSessionFactory().getCurrentSession();
        String queryStr = String.format(
                " FROM %s " //
                        + " WHERE state = :state " //
                        + " ORDER BY created DESC ", //
                getEntityClass().getSimpleName());
        Query query = session.createQuery(queryStr);
        query.setString("state", state.name());
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
    @SuppressWarnings("unchecked")
    public List<PlayLaunch> findByPlayStatesAndPagination(Long playId, List<LaunchState> states, Long startTimestamp,
            Long offset, Long max, String sortby, boolean descending, Long endTimestamp) {
        Session session = getSessionFactory().getCurrentSession();
        Class<PlayLaunch> entityClz = getEntityClass();

        String queryStr = "";
        Query query = createQueryForDashboard(playId, states, startTimestamp, offset, max, sortby, descending,
                endTimestamp, session, entityClz, queryStr, true);

        return query.list();
    }

    @Override
    public Long findCountByPlayStatesAndTimestamps(Long playId, List<LaunchState> states, Long startTimestamp,
            Long endTimestamp) {
        Session session = getSessionFactory().getCurrentSession();
        Class<PlayLaunch> entityClz = getEntityClass();

        String queryStr = "SELECT count(*) ";
        Query query = createQueryForDashboard(playId, states, startTimestamp, null, null, null, true, endTimestamp,
                session, entityClz, queryStr, false);

        return Long.parseLong(query.uniqueResult().toString());
    }

    @Override
    @SuppressWarnings("unchecked")
    public List<Play> findDashboardPlaysWithLaunches(Long playId, List<LaunchState> states, Long startTimestamp,
            Long endTimestamp) {
        Session session = getSessionFactory().getCurrentSession();
        Class<PlayLaunch> entityClz = getEntityClass();

        String queryStr = "FROM Play WHERE pid IN ( SELECT distinct play ";
        Query query = createQueryForDashboard(playId, states, startTimestamp, null, null, null, true, endTimestamp,
                session, entityClz, queryStr, ")", false);
        return query.list();
    }

    @Override
    @SuppressWarnings("unchecked")
    public Stats findTotalCountByPlayStatesAndTimestamps(Long playId, List<LaunchState> states, Long startTimestamp,
            Long endTimestamp) {
        Session session = getSessionFactory().getCurrentSession();
        Class<PlayLaunch> entityClz = getEntityClass();
        String totalAccountsSelected = "totalAccountsSelected";
        String totalAccountsLaunched = "totalAccountsLaunched";
        String totalAccountsSuppressed = "totalAccountsSuppressed";
        String totalAccountsErrored = "totalAccountsErrored";
        String totalContactsLaunched = "totalContactsLaunched";

        String queryStr = "SELECT new map " + "( " //
                + " SUM(COALESCE(accountsSelected)) AS " + totalAccountsSelected + ", " //
                + " SUM(COALESCE(accountsLaunched)) AS " + totalAccountsLaunched + ", " //
                + " SUM(COALESCE(accountsSuppressed)) AS " + totalAccountsSuppressed + ", " //
                + " SUM(COALESCE(accountsErrored)) AS " + totalAccountsErrored + ", " //
                + " SUM(COALESCE(contactsLaunched)) AS " + totalContactsLaunched + " " + ") ";

        Query query = createQueryForDashboard(playId, states, startTimestamp, null, null, null, true, endTimestamp,
                session, entityClz, queryStr, false);

        List<Map<String, Object>> queryResult = query.list();
        Stats totalCounts = new Stats();
        Map<String, Object> res = queryResult.get(0);
        totalCounts.setSelectedTargets(getVal(res, totalAccountsSelected));
        totalCounts.setRecommendationsLaunched(getVal(res, totalAccountsLaunched));
        totalCounts.setSuppressed(getVal(res, totalAccountsSuppressed));
        totalCounts.setErrors(getVal(res, totalAccountsErrored));
        totalCounts.setContactsWithinRecommendations(getVal(res, totalContactsLaunched));

        return totalCounts;
    }

    private Long getVal(Map<String, Object> resMap, String key) {
        Object val = resMap.get(key);
        return val == null ? 0L : (Long) val;
    }

    private Query createQueryForDashboard(Long playId, List<LaunchState> states, Long startTimestamp, Long offset,
            Long max, String sortby, boolean descending, Long endTimestamp, Session session,
            Class<PlayLaunch> entityClz, String queryStr, boolean sortNeeded) {
        return createQueryForDashboard(playId, states, startTimestamp, offset, max, sortby, descending, endTimestamp,
                session, entityClz, queryStr, null, sortNeeded);
    }

    private Query createQueryForDashboard(Long playId, List<LaunchState> states, Long startTimestamp, Long offset,
            Long max, String sortby, boolean descending, Long endTimestamp, Session session,
            Class<PlayLaunch> entityClz, String queryStr, String closingQueryStr, boolean sortNeeded) {
        queryStr += String.format(
                " FROM %s "//
                        + " WHERE created >= :startTimestamp ", //
                entityClz.getSimpleName());

        if (endTimestamp != null) {
            queryStr += " AND created <= :endTimestamp  ";
        }

        if (playId != null) {
            queryStr += " AND fk_play_id = :playId ";
        }

        if (CollectionUtils.isNotEmpty(states)) {
            queryStr += " AND state IN ( :states ) ";
        }

        if (sortNeeded) {
            if (StringUtils.isBlank(sortby)) {
                sortby = CREATED_COL;
            } else if (sortby.trim().equalsIgnoreCase(CREATED_COL)) {
                sortby = String.format("%s, %s ", sortby, CREATED_COL);
            }

            queryStr += String.format( //
                    " ORDER BY %s %s ", //
                    sortby, descending ? "DESC" : "ASC");
        }

        if (StringUtils.isNotBlank(closingQueryStr)) {
            queryStr += closingQueryStr;
        }

        Query query = session.createQuery(queryStr);
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
}
