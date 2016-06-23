package com.latticeengines.security.dao.impl;

import java.util.ArrayList;
import java.util.List;

import org.hibernate.Query;
import org.hibernate.Session;
import org.springframework.stereotype.Component;

import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.auth.GlobalAuthUserTenantRight;
import com.latticeengines.security.dao.GlobalAuthUserTenantRightDao;

@Component("globalAuthUserTenantRightDao")
public class GlobalAuthUserTenantRightDaoImpl extends BaseDaoImpl<GlobalAuthUserTenantRight>
        implements
        GlobalAuthUserTenantRightDao {

    @SuppressWarnings("rawtypes")
    @Override
    public List<GlobalAuthUserTenantRight> findByUserIdAndTenantId(Long userId, Long tenantId) {
        Session session = sessionFactory.getCurrentSession();
        Class<GlobalAuthUserTenantRight> entityClz = getEntityClass();
        String queryStr = String.format("from %s",
                entityClz.getSimpleName(),
                tenantId);
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
                "from %s where Operation_Name = '%s' and Tenant_ID = %d",
                entityClz.getSimpleName(),
                operationName, tenantId);
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

}
