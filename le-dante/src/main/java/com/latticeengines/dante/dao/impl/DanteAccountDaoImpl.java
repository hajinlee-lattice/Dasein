package com.latticeengines.dante.dao.impl;

import java.util.List;

import org.hibernate.Session;
import org.hibernate.query.Query;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.dante.dao.DanteAccountDao;
import com.latticeengines.dantedb.exposed.dao.impl.BaseDanteDaoImpl;
import com.latticeengines.domain.exposed.dante.DanteAccount;

/**
 * Retrieves Accounts for Talking Point UI from Dante database. Starting from
 * M16 we should not use this. Objectapi should be used instead
 */
@Deprecated
@Component("danteAccountDao")
public class DanteAccountDaoImpl extends BaseDanteDaoImpl<DanteAccount> implements DanteAccountDao {

    @SuppressWarnings("unused")
    private static final Logger log = LoggerFactory.getLogger(DanteAccountDaoImpl.class);

    @Override
    protected Class<DanteAccount> getEntityClass() {
        return DanteAccount.class;
    }

    @SuppressWarnings("unchecked")
    public List<DanteAccount> getAccounts(int count, String customerID) {
        Session session = getSessionFactory().getCurrentSession();
        String queryStr = String.format("select a from %s a where customerID = :customerID",
                getEntityClass().getSimpleName());
        Query<DanteAccount> query = session.createQuery(queryStr).setMaxResults(count);
        query.setParameter("customerID", customerID);
        return (List<DanteAccount>) query.list();
    }
}
