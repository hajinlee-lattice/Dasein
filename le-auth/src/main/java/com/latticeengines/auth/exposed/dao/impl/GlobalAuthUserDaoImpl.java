package com.latticeengines.auth.exposed.dao.impl;

import java.util.HashMap;
import java.util.List;

import org.hibernate.Hibernate;
import org.hibernate.Session;
import org.hibernate.query.Query;
import org.springframework.stereotype.Component;

import com.latticeengines.auth.exposed.dao.GlobalAuthUserDao;
import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.auth.GlobalAuthTenant;
import com.latticeengines.domain.exposed.auth.GlobalAuthUser;

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
        String queryStr = String.format(
                "from %s as gauser left outer join fetch gauser.gaAuthentications where Email = :email",
                entityClz.getSimpleName());
        Query query = session.createQuery(queryStr);
        query.setParameter("email", email);
        List list = query.list();
        if (list.size() == 0) {
            return null;
        } else {
            return (GlobalAuthUser) list.get(0);
        }
    }

    @Override
    public HashMap<Long, String> findUserInfoByTenant(GlobalAuthTenant tenant) {
        Session session = sessionFactory.getCurrentSession();
        String sqlStr = String.format("SELECT gaUser FROM %s as gaUser " //
                + "INNER JOIN gaUser.gaUserTenantRights as gaRight " //
                + "WHERE gaRight.globalAuthTenant = :tenant", //
                getEntityClass().getSimpleName());
        Query<?> query = session.createQuery(sqlStr);
        query.setParameter("tenant", tenant);
        List<?> list = query.list();
        HashMap<Long, String> userInfos = new HashMap<>();
        if (list.size() == 0) {
            return null;
        } else {
            for (Object obj : list) {
                GlobalAuthUser user = (GlobalAuthUser) obj;
                userInfos.put(user.getPid(), user.getEmail());
            }
            return userInfos;
        }
    }

}
