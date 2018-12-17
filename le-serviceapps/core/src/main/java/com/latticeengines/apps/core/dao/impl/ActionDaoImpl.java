package com.latticeengines.apps.core.dao.impl;

import java.util.Collections;
import java.util.List;

import org.hibernate.Session;
import org.hibernate.query.Query;
import org.springframework.stereotype.Component;

import com.latticeengines.apps.core.dao.ActionDao;
import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.pls.Action;

@Component("actionDao")
public class ActionDaoImpl extends BaseDaoImpl<Action> implements ActionDao {

    private static final Boolean is_CANCEL = true;

    @Override
    protected Class<Action> getEntityClass() {
        return Action.class;
    }

    @Override
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public List<Action> findAllWithNullOwnerId() {
        Session session = getSessionFactory().getCurrentSession();
        String queryStr = String.format("from %s where OWNER_ID is null", getEntityClass().getSimpleName());

        Query query = session.createQuery(queryStr);
        List<Action> results = query.list();
        if (results.size() == 0) {
            return Collections.emptyList();
        }
        return results;
    }

    @SuppressWarnings("rawtypes")
    @Override
    public void updateOwnerIdIn(Long ownerId, List<Long> actionPids) {
        Session session = getSessionFactory().getCurrentSession();
        String queryStr = String.format("update %s set OWNER_ID = :ownerId where PID in (:actionPids)",
                getEntityClass().getSimpleName());
        Query query = session.createQuery(queryStr);
        query.setParameter("ownerId", ownerId);
        query.setParameterList("actionPids", actionPids);
        query.executeUpdate();
    }

    @SuppressWarnings("rawtypes")
    @Override
    public void cancel(Long actionPid) {
        Session session = getSessionFactory().getCurrentSession();
        String queryStr = String.format("update %s set canceled = :canceled where PID = :actionPid",
                getEntityClass().getSimpleName());
        Query query = session.createQuery(queryStr);
        query.setParameter("canceled", is_CANCEL);
        query.setParameter("actionPid", actionPid);
        query.executeUpdate();
    }

}
