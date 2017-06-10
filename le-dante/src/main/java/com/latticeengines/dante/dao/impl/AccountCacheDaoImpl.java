package com.latticeengines.dante.dao.impl;

import java.util.List;

import org.apache.log4j.Logger;
import org.hibernate.Query;
import org.hibernate.Session;
import org.springframework.stereotype.Component;

import com.latticeengines.dante.dao.AccountCacheDao;
import com.latticeengines.dantedb.exposed.dao.impl.BaseDanteDaoImpl;
import com.latticeengines.domain.exposed.dante.DanteAccount;

@Component("accountCacheDao")
public class AccountCacheDaoImpl extends BaseDanteDaoImpl<DanteAccount> implements AccountCacheDao {

    private static final Logger log = Logger.getLogger(AccountCacheDaoImpl.class);

    @Override
    protected Class<DanteAccount> getEntityClass() {
        return DanteAccount.class;
    }

    @SuppressWarnings("unchecked")
    public List<DanteAccount> getAccounts(int count, String customerID) {
        Session session = getSessionFactory().getCurrentSession();
        String queryStr = String.format("select a from %s a where customerID = :customerID",
                getEntityClass().getSimpleName());
        Query query = session.createQuery(queryStr).setMaxResults(count);
        query.setParameter("customerID", customerID);
        return (List<DanteAccount>) query.list();
    }
}
