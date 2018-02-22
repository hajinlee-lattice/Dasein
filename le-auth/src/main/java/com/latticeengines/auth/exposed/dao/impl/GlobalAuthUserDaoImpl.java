package com.latticeengines.auth.exposed.dao.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import javax.persistence.Table;

import org.hibernate.Hibernate;
import org.hibernate.SQLQuery;
import org.hibernate.query.Query;
import org.hibernate.type.LongType;
import org.hibernate.type.StringType;
import org.hibernate.Session;
import org.springframework.stereotype.Component;

import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.auth.GlobalAuthAuthentication;
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

    private Class<GlobalAuthUserTenantRight> getUserRightClass() {
        return GlobalAuthUserTenantRight.class;
    }
    
    @SuppressWarnings("rawtypes")
    @Override
    public GlobalAuthUser findByEmailJoinAuthentication(String email) {
        Session session = sessionFactory.getCurrentSession();
        Class<GlobalAuthUser> entityClz = getEntityClass();
        String queryStr = String.format("from %s as gauser left outer join fetch gauser.gaAuthentications where Email = '%s'",
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

    @SuppressWarnings("unchecked")
    @Override
    public HashMap<Long, String> findUserInfoByTenantId(Long tenantId) {
        Session session = sessionFactory.getCurrentSession();
        Class<GlobalAuthUser> entityClz = getEntityClass();
        Class<GlobalAuthUserTenantRight> userRightClz = getUserRightClass();
        String gaUserTable = entityClz.getAnnotation(Table.class).name();
        String gaUserRightTable = userRightClz.getAnnotation(Table.class).name();
        String sqlStr = String.format("SELECT gaUser.GlobalUser_ID, gaUser.Email " +
                "FROM %s as gaUser JOIN %s as gaUserRight " +
                "ON gaUser.GlobalUser_ID = gaUserRight.User_Id " +
                "WHERE gaUserRight.Tenant_Id = :tenantId", gaUserTable, gaUserRightTable);
        SQLQuery query = session.createSQLQuery(sqlStr)
                .addScalar("GlobalUser_ID", new LongType())
                .addScalar("Email", new StringType());
        query.setLong("tenantId", tenantId);
        List<Object[]> list = query.list();
        HashMap<Long, String> userInfos = new HashMap<>();
        if (list.size() == 0) {
            return null;
        } else {
            for (Object[] auth: list) {
                userInfos.put(Long.parseLong(auth[0].toString()), auth[1].toString());
            }
            return userInfos;
        }
    }

}
