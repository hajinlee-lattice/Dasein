package com.latticeengines.pls.dao.impl;

import java.util.Collections;
import java.util.List;

import org.hibernate.Session;
import org.hibernate.query.Query;
import org.springframework.stereotype.Component;

import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.pls.Action;
import com.latticeengines.pls.dao.ActionDao;

@Component("actionDao")
public class ActionDaoImpl extends BaseDaoImpl<Action> implements ActionDao {

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

}
