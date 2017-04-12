package com.latticeengines.auth.exposed.dao.impl;

import java.util.HashMap;
import java.util.List;

import javax.persistence.Table;

import org.hibernate.Query;
import org.hibernate.SQLQuery;
import org.hibernate.Session;
import org.hibernate.type.LongType;
import org.hibernate.type.StringType;
import org.springframework.stereotype.Component;

import com.latticeengines.auth.exposed.dao.GlobalAuthAuthenticationDao;
import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.auth.GlobalAuthAuthentication;
import com.latticeengines.domain.exposed.auth.GlobalAuthUserTenantRight;

@Component("globalAuthAuthenticationDao")
public class GlobalAuthAuthenticationDaoImpl extends BaseDaoImpl<GlobalAuthAuthentication>
        implements
        GlobalAuthAuthenticationDao {

    @Override
    protected Class<GlobalAuthAuthentication> getEntityClass() {
        return GlobalAuthAuthentication.class;
    }

    private Class<GlobalAuthUserTenantRight> getUserRightClass() {
        return GlobalAuthUserTenantRight.class;
    }

    @SuppressWarnings("rawtypes")
    @Override
    public GlobalAuthAuthentication findByUsernameJoinUser(String username) {
        Session session = sessionFactory.getCurrentSession();
        Class<GlobalAuthAuthentication> entityClz = getEntityClass();
        String queryStr = String.format("from %s where Username = '%s'",
                entityClz.getSimpleName(),
                username);
        Query query = session.createQuery(queryStr);
        List list = query.list();
        if (list.size() == 0) {
            return null;
        } else {
            for (Object auth : list) {
                GlobalAuthAuthentication gaAuth = (GlobalAuthAuthentication) auth;
                if (gaAuth.getGlobalAuthUser() != null) {
                    return gaAuth;
                }
            }
            return (GlobalAuthAuthentication) list.get(0);
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public HashMap<Long, String> findUserInfoByTenantId(Long tenantId) {
        Session session = sessionFactory.getCurrentSession();
        Class<GlobalAuthAuthentication> entityClz = getEntityClass();
        Class<GlobalAuthUserTenantRight> userRightClz = getUserRightClass();
        String gaAuthTable = entityClz.getAnnotation(Table.class).name();
        String gaUserRightTable = userRightClz.getAnnotation(Table.class).name();
        String sqlStr = String.format("SELECT gaAuth.User_Id, gaAuth.Username " +
                "FROM %s as gaAuth JOIN %s as gaUserRight " +
                "ON gaAuth.User_Id = gaUserRight.User_Id " +
                "WHERE gaUserRight.Tenant_Id = :tenantId", gaAuthTable, gaUserRightTable);
        SQLQuery query = session.createSQLQuery(sqlStr)
                .addScalar("User_Id", new LongType())
                .addScalar("Username", new StringType());
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

    @Override
    public Boolean deleteByUserId(Long userId) {
        Session session = sessionFactory.getCurrentSession();
        Class<GlobalAuthAuthentication> entityClz = getEntityClass();
        String sqlStr = String.format("delete from %s where User_ID = %d", entityClz.getAnnotation(Table.class).name(), userId);
        SQLQuery query = session.createSQLQuery(sqlStr);
        query.executeUpdate();
        return true;
    }
}
