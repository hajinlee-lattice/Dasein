package com.latticeengines.auth.exposed.dao.impl;

import java.util.ArrayList;
import java.util.List;


import org.apache.commons.collections4.CollectionUtils;
import org.hibernate.Session;
import org.hibernate.query.Query;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.auth.exposed.dao.GlobalAuthUserTenantRightDao;
import com.latticeengines.common.exposed.util.HibernateUtils;
import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.auth.GlobalAuthUser;
import com.latticeengines.domain.exposed.auth.GlobalAuthUserTenantRight;

@Component("globalAuthUserTenantRightDao")
public class GlobalAuthUserTenantRightDaoImpl extends BaseDaoImpl<GlobalAuthUserTenantRight>
        implements GlobalAuthUserTenantRightDao {

    private static final Logger log = LoggerFactory.getLogger(GlobalAuthUserTenantRightDaoImpl.class);

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
                GlobalAuthUser gaUser = HibernateUtils.inflateDetails(gaTenantRight.getGlobalAuthUser());
                users.add(gaUser);
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
            for (Object obj : list) {
                GlobalAuthUserTenantRight gaTenantRight = (GlobalAuthUserTenantRight) obj;
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
        String sqlStr = String.format("delete from %s where User_ID = %d", entityClz.getSimpleName(), userId);
        Query<?> query = session.createQuery(sqlStr);
        query.executeUpdate();
        return true;
    }

    @SuppressWarnings("unchecked")
    @Override
    public List<GlobalAuthUserTenantRight> findByEmail(String email) {
        Session session = sessionFactory.getCurrentSession();
        Class<GlobalAuthUserTenantRight> entityClz = getEntityClass();
        String queryPattern = "from %s where globalAuthUser.email = :email";
        String queryStr = String.format(queryPattern, entityClz.getSimpleName());
        Query<?> query = session.createQuery(queryStr);
        query.setParameter("email", email);
        return (List<GlobalAuthUserTenantRight>) query.list();
    }

    @Override
    public boolean existsByEmail(String email) {
        Session session = sessionFactory.getCurrentSession();
        Class<GlobalAuthUserTenantRight> entityClz = getEntityClass();
        String queryPattern = "from %s where globalAuthUser.email = :email";
        String queryStr = String.format(queryPattern, entityClz.getSimpleName());
        Query<?> query = session.createQuery(queryStr);
        query.setParameter("email", email);
        query.setMaxResults(1);
        return CollectionUtils.isNotEmpty(query.list());
    }

    @SuppressWarnings("unchecked")
    @Override
    public List<GlobalAuthUserTenantRight> findByNonNullExprationDate() {
        Session session = sessionFactory.getCurrentSession();
        Class<GlobalAuthUserTenantRight> entityClz = getEntityClass();
        String queryStr = String.format("from %s where Expiration_Date is not null", entityClz.getSimpleName());
        Query<?> query = session.createQuery(queryStr);
        return (List<GlobalAuthUserTenantRight>) query.list();
    }

}
