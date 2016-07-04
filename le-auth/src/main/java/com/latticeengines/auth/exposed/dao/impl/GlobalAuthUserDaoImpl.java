package com.latticeengines.auth.exposed.dao.impl;

import java.util.ArrayList;
import java.util.List;

import org.hibernate.Hibernate;
import org.hibernate.Query;
import org.hibernate.Session;
import org.springframework.stereotype.Component;

import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.auth.GlobalAuthUser;
import com.latticeengines.domain.exposed.auth.GlobalAuthUserTenantRight;
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
        String queryStr = String.format("from %s where Email = '%s'",
                entityClz.getSimpleName(),
                email);
        Query query = session.createQuery(queryStr);
        List list = query.list();
        if (list.size() == 0) {
            return null;
        } else {
            for (Object user : list) {
                GlobalAuthUser gaUser = (GlobalAuthUser) user;
                Hibernate.initialize(gaUser.getAuthentications());
                if ((gaUser.getAuthentications() != null)
                        && (gaUser.getAuthentications().size() > 0)) {
                    return gaUser;
                }
            }
            return null;
        }
    }

    @SuppressWarnings("rawtypes")
    @Override
    public List<GlobalAuthUser> findByEmailJoinUserTenantRight(String email) {
        Session session = sessionFactory.getCurrentSession();
        Class<GlobalAuthUser> entityClz = getEntityClass();
        String queryStr = String.format("from %s where Email = '%s'",
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
                Hibernate.initialize(gaUser.getUserTenantRights());
                if ((gaUser.getUserTenantRights() != null)
                        && (gaUser.getUserTenantRights().size() > 0)) {
                    gaUsers.add(gaUser);
                }
            }
            return gaUsers;
        }
    }

    @SuppressWarnings("rawtypes")
    @Override
    public List<GlobalAuthUser> findByTenantIdJoinAuthenticationJoinUserTenantRight(Long tenantId) {
        Session session = sessionFactory.getCurrentSession();
        Class<GlobalAuthUser> entityClz = getEntityClass();
        String queryStr = String.format("from %s ",
                entityClz.getSimpleName());
        Query query = session.createQuery(queryStr);
        List list = query.list();
        List<GlobalAuthUser> gaUsers = new ArrayList<GlobalAuthUser>();
        if (list.size() == 0) {
            return null;
        } else {
            for (Object user : list) {
                GlobalAuthUser gaUser = (GlobalAuthUser) user;
                Hibernate.initialize(gaUser.getAuthentications());
                Hibernate.initialize(gaUser.getUserTenantRights());
                if ((gaUser.getAuthentications() != null)
                        && (gaUser.getAuthentications().size() > 0)) {
                    for (GlobalAuthUserTenantRight userRightData : gaUser.getUserTenantRights()) {
                        if (userRightData.getGlobalAuthTenant() != null
                                && userRightData.getGlobalAuthTenant().getPid().equals(tenantId)) {
                            gaUsers.add(gaUser);
                        }
                    }
                }
            }
            return gaUsers;
        }
    }
}
