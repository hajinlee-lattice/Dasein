package com.latticeengines.auth.exposed.dao.impl;

import java.util.ArrayList;
import java.util.List;

import org.hibernate.Hibernate;
import org.hibernate.Query;
import org.hibernate.Session;
import org.springframework.stereotype.Component;

import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.auth.GlobalAuthUser;
import com.latticeengines.auth.exposed.dao.GlobalAuthUserDao;

@Component("globalAuthUserDao")
public class GlobalAuthUserDaoImpl extends BaseDaoImpl<GlobalAuthUser> implements GlobalAuthUserDao {

    @Override
    public GlobalAuthUser findByUserIdWithTenantRightsAndAuthentications(Long userId) {
        GlobalAuthUser gaUser = super.findByField("GlobalUser_ID", userId);
        if (gaUser != null) {
            Hibernate.initialize(gaUser.getAuthentications());
            Hibernate.initialize(gaUser.getUserTenantRights());
        }
        return gaUser;
    }

    @Override
    protected Class<GlobalAuthUser> getEntityClass() {
        return GlobalAuthUser.class;
    }

    @SuppressWarnings("rawtypes")
    @Override
    public GlobalAuthUser findByEmailJoinAuthentication(String email) {
        Session session = sessionFactory.getCurrentSession();
        Class<GlobalAuthUser> entityClz = getEntityClass();
        String queryStr = String.format("from %s as gauser inner join fetch gauser.gaAuthentications where Email = '%s'",
                entityClz.getSimpleName(),
                email);
        Query query = session.createQuery(queryStr);
        List list = query.list();
        if (list.size() == 0) {
            return null;
        } else {
            return (GlobalAuthUser) list.get(0);
        }
    }

    @SuppressWarnings("rawtypes")
    @Override
    public List<GlobalAuthUser> findByEmailJoinUserTenantRight(String email) {
        Session session = sessionFactory.getCurrentSession();
        Class<GlobalAuthUser> entityClz = getEntityClass();
        String queryStr = String.format("from %s as gauser inner join fetch gauser.gaUserTenantRights where Email = '%s'",
                entityClz.getSimpleName(),
                email);
        Query query = session.createQuery(queryStr);
        List list = query.list();
        List<GlobalAuthUser> gaUsers = new ArrayList<GlobalAuthUser>();
        if (list.size() == 0) {
            return null;
        } else {
            for (Object user : list) {
                GlobalAuthUser gaUser = (GlobalAuthUser) user;
                gaUsers.add(gaUser);
            }
            return gaUsers;
        }
    }

}
