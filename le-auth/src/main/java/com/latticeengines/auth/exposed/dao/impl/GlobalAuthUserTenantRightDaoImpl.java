package com.latticeengines.auth.exposed.dao.impl;

import java.util.ArrayList;
import java.util.List;

import javax.persistence.Table;

import com.latticeengines.domain.exposed.auth.GlobalAuthUser;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.hibernate.Query;
import org.hibernate.SQLQuery;
import org.hibernate.Session;
import org.springframework.stereotype.Component;

import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.auth.GlobalAuthUserTenantRight;
import com.latticeengines.auth.exposed.dao.GlobalAuthUserTenantRightDao;

@Component("globalAuthUserTenantRightDao")
public class GlobalAuthUserTenantRightDaoImpl extends BaseDaoImpl<GlobalAuthUserTenantRight>
        implements GlobalAuthUserTenantRightDao {

    private static final Log log = LogFactory.getLog(GlobalAuthUserTenantRightDaoImpl.class);

    @SuppressWarnings("rawtypes")
    @Override
    public List<GlobalAuthUserTenantRight> findByUserIdAndTenantId(Long userId, Long tenantId) {
        try {
            Session session = sessionFactory.getCurrentSession();
            Class<GlobalAuthUserTenantRight> entityClz = getEntityClass();
            String queryStr = String.format("from %s where User_ID = %d and Tenant_ID = %d", entityClz.getSimpleName(),
                    userId, tenantId);
            Query query = session.createQuery(queryStr);
            List list = query.list();
            List<GlobalAuthUserTenantRight> userTenantRightDataList = new ArrayList<GlobalAuthUserTenantRight>();
            if (list.size() == 0) {
                return null;
            } else {
                for (int i = 0; i < list.size(); i++) {
                    GlobalAuthUserTenantRight gaTenantRight = (GlobalAuthUserTenantRight) list.get(i);
                    if (gaTenantRight.getGlobalAuthUser() != null
                            && gaTenantRight.getGlobalAuthUser().getPid().equals(userId)) {
                        if (gaTenantRight.getGlobalAuthTenant() != null
                                && gaTenantRight.getGlobalAuthTenant().getPid().equals(tenantId)) {
                            userTenantRightDataList.add(gaTenantRight);
                        }
                    }
                }
            }
            return userTenantRightDataList;
        } catch (Exception e) {
            log.error(String.format("Find tenant right by userId: %d, tenantId: %d failed.", userId, tenantId));
            throw e;
        }
    }

    @SuppressWarnings("rawtypes")
    @Override
    public List<GlobalAuthUser> findUsersByTenantId(Long tenantId) {
        Session session = sessionFactory.getCurrentSession();
        Class<GlobalAuthUserTenantRight> entityClz = getEntityClass();
        String queryStr = String.format(
                "from %s where Tenant_ID = %d",
                entityClz.getSimpleName(),
                tenantId);
        Query query = session.createQuery(queryStr);
        List list = query.list();
        List<GlobalAuthUser> users = new ArrayList<GlobalAuthUser>();
        if (list.size() == 0) {
            return null;
        } else {
            for (int i = 0; i < list.size(); i++) {
                GlobalAuthUserTenantRight gaTenantRight = (GlobalAuthUserTenantRight) list.get(i);
                users.add(gaTenantRight.getGlobalAuthUser());
            }
            return users;
        }
    }

    @Override
    protected Class<GlobalAuthUserTenantRight> getEntityClass() {
        return GlobalAuthUserTenantRight.class;
    }

    @SuppressWarnings("rawtypes")
    @Override
    public GlobalAuthUserTenantRight findByUserIdAndTenantIdAndOperationName(Long userId,
            Long tenantId, String operationName) {
        Session session = sessionFactory.getCurrentSession();
        Class<GlobalAuthUserTenantRight> entityClz = getEntityClass();
        String queryStr = String.format(
                "from %s where Operation_Name = '%s' and User_ID = %d and Tenant_ID = %d",
                entityClz.getSimpleName(),
                operationName, userId, tenantId);
        Query query = session.createQuery(queryStr);
        List list = query.list();
        if (list.size() == 0) {
            return null;
        } else {
            for (int i = 0; i < list.size(); i++) {
                GlobalAuthUserTenantRight gaTenantRight = (GlobalAuthUserTenantRight) list.get(i);
                if (gaTenantRight.getGlobalAuthUser() != null
                        && gaTenantRight.getGlobalAuthUser().getPid().equals(userId)) {
                    if (gaTenantRight.getGlobalAuthTenant() != null
                            && gaTenantRight.getGlobalAuthTenant().getPid().equals(tenantId)) {
                        return gaTenantRight;
                    }
                }
            }
        }
        return null;
    }

    @Override
    public Boolean deleteByUserId(Long userId) {
        Session session = sessionFactory.getCurrentSession();
        Class<GlobalAuthUserTenantRight> entityClz = getEntityClass();
        String sqlStr = String.format("delete from %s where User_ID = %d", entityClz.getAnnotation(Table.class).name(), userId);
        SQLQuery query = session.createSQLQuery(sqlStr);
        query.executeUpdate();
        return true;
    }

}
