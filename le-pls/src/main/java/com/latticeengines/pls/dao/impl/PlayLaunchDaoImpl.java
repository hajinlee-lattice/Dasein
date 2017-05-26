package com.latticeengines.pls.dao.impl;

import java.util.Date;
import java.util.List;

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
    public PlayLaunch findByName(String name) {
        Session session = getSessionFactory().getCurrentSession();
        Class<PlayLaunch> entityClz = getEntityClass();
        String queryStr = String.format("from %s where name = :playLaunchName", entityClz.getSimpleName());
        Query query = session.createQuery(queryStr);
        query.setString("playLaunchName", name);
        List list = query.list();
        if (list.size() == 0) {
            return null;
        }
        return (PlayLaunch) list.get(0);
    }

    @SuppressWarnings("rawtypes")
    @Override
    public PlayLaunch findByLaunchId(String launchId) {
        Session session = getSessionFactory().getCurrentSession();
        Class<PlayLaunch> entityClz = getEntityClass();
        String queryStr = String.format("from %s where launch_id = :launchId", entityClz.getSimpleName());
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
    public PlayLaunch findByPlayAndTimestamp(Long playId, Date timestamp) {
        Session session = getSessionFactory().getCurrentSession();
        Class<PlayLaunch> entityClz = getEntityClass();
        String queryStr = String.format(
                "from %s where "//
                        + "fk_play_id = :playId AND timestamp = :timestamp", //
                entityClz.getSimpleName());
        Query query = session.createQuery(queryStr);
        query.setLong("fk_play_id", playId);
        query.setDate("timestamp", timestamp);
        List list = query.list();
        if (list.size() == 0) {
            return null;
        }
        return (PlayLaunch) list.get(0);
    }

    @SuppressWarnings("unchecked")
    @Override
    public List<PlayLaunch> findByPlayId(Long playId, LaunchState state) {
        Session session = getSessionFactory().getCurrentSession();
        Class<PlayLaunch> entityClz = getEntityClass();
        String queryStr = String.format(
                "from %s where "//
                        + "fk_play_id = :playId", //
                entityClz.getSimpleName());
        if (state != null) {
            queryStr += " AND state = :state";
        }
        Query query = session.createQuery(queryStr);
        query.setLong("playId", playId);
        if (state != null) {
            query.setString("state", state.name());
        }
        return query.list();
    }

    @SuppressWarnings("unchecked")
    @Override
    public List<PlayLaunch> findByState(LaunchState state) {
        Session session = getSessionFactory().getCurrentSession();
        String queryStr = String.format("from %s where state = :state", //
                getEntityClass().getSimpleName());
        Query query = session.createQuery(queryStr);
        query.setString("state", state.name());
        return query.list();
    }
}
