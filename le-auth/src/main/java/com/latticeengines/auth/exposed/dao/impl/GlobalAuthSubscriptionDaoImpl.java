package com.latticeengines.auth.exposed.dao.impl;

import java.util.ArrayList;
import java.util.List;

import org.hibernate.Session;
import org.hibernate.query.Query;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.auth.exposed.dao.GlobalAuthSubscriptionDao;
import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.auth.GlobalAuthSubscription;

@Component("globalAuthSubscriptionDao")
public class GlobalAuthSubscriptionDaoImpl extends BaseDaoImpl<GlobalAuthSubscription>
        implements GlobalAuthSubscriptionDao {

    private static final Logger log = LoggerFactory.getLogger(GlobalAuthSubscriptionDaoImpl.class);

    @Override
    protected Class<GlobalAuthSubscription> getEntityClass() {
        return GlobalAuthSubscription.class;
    }

    @Override
    public GlobalAuthSubscription findByEmailAndTenantId(String email, String tenantId) {
        Session session = sessionFactory.getCurrentSession();
        Class<GlobalAuthSubscription> entityClz = getEntityClass();
        String queryPattern = "from %s where gaUserTenantRight.globalAuthUser.email = :email and tenantId = :tenantId";
        String queryStr = String.format(queryPattern, entityClz.getSimpleName(), tenantId);
        Query<?> query = session.createQuery(queryStr);
        query.setParameter("email", email);
        query.setParameter("tenantId", tenantId);
        List list = query.list();
        if (list.size() == 0) {
            return null;
        } else {
            return (GlobalAuthSubscription) list.get(0);
        }
    }

    @Override
    public List<String> findEmailsByTenantId(String tenantId) {
        Session session = sessionFactory.getCurrentSession();
        Class<GlobalAuthSubscription> entityClz = getEntityClass();
        String queryStr = String.format("from %s where Tenant_ID = :tenantId", entityClz.getSimpleName());
        Query query = session.createQuery(queryStr);
        query.setParameter("tenantId", tenantId);
        List list = query.list();
        List<String> emails = new ArrayList<>();
        for (int i = 0; i < list.size(); i++) {
            GlobalAuthSubscription subscription = (GlobalAuthSubscription) list.get(i);
            String email = subscription.getUserTenantRight().getGlobalAuthUser().getEmail();
            emails.add(email);
        }
        return emails;
    }

}
