package com.latticeengines.pls.dao.impl;

import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.hibernate.Query;
import org.hibernate.Session;
import org.springframework.stereotype.Component;

import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.pls.LaunchState;
import com.latticeengines.domain.exposed.pls.PlayLaunch;
import com.latticeengines.pls.dao.PlayLaunchDao;

@Component("playLaunchDao")
public class PlayLaunchDaoImpl extends BaseDaoImpl<PlayLaunch> implements PlayLaunchDao {

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
}
