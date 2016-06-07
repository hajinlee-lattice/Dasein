package com.latticeengines.quartzclient.dao.impl;

import java.util.List;

import org.hibernate.HibernateException;
import org.hibernate.Query;
import org.hibernate.Session;
import org.springframework.stereotype.Component;

import com.latticeengines.db.exposed.dao.impl.BaseDaoWithAssignedSessionFactoryImpl;
import com.latticeengines.domain.exposed.quartz.ActiveStack;
import com.latticeengines.quartzclient.dao.ActiveStackDao;

@Component("activeStackDao")
public class ActiveStackDaoImpl extends BaseDaoWithAssignedSessionFactoryImpl<ActiveStack> implements ActiveStackDao {

    public ActiveStackDaoImpl() {
        super();
    }

    @SuppressWarnings("rawtypes")
    @Override
    public String getActiveStack() {
        Session session = getSession();
        Class<ActiveStack> entityClz = getEntityClass();
        String queryStr = String
                .format(
                        "from %s ", entityClz.getSimpleName());
        Query query = session.createQuery(queryStr);
        List list = query.list();
        session.close();
        if (list.size() == 0) {
            return null;
        } else {
            ActiveStack activeStack = (ActiveStack) list.get(0);
            return activeStack.getActiveStack();
        }
    }

    @Override
    protected Class<ActiveStack> getEntityClass() {
        return ActiveStack.class;
    }

    private Session getSession() throws HibernateException {
        Session sess = null;
        try {
            sess = getSessionFactory().getCurrentSession();
        } catch (HibernateException e) {
            sess = getSessionFactory().openSession();
        }
        return sess;
    }

}
