package com.latticeengines.datacloud.core.dao.impl;

import java.util.List;

import org.hibernate.Session;
import org.hibernate.query.Query;

import com.latticeengines.datacloud.core.dao.AccountMasterFactDao;
import com.latticeengines.db.exposed.dao.impl.BaseDaoWithAssignedSessionFactoryImpl;
import com.latticeengines.domain.exposed.datacloud.manage.AccountMasterFact;

public class AccountMasterFactDaoImpl extends BaseDaoWithAssignedSessionFactoryImpl<AccountMasterFact>
        implements AccountMasterFactDao {

    @Override
    protected Class<AccountMasterFact> getEntityClass() {
        return AccountMasterFact.class;
    }

    @Override
    public AccountMasterFact findByDimensions(Long location, Long industry, Long numEmpRange, Long revRange,
            Long numLocRange, Long category) {
        Session session = sessionFactory.getCurrentSession();
        String queryStr = String.format("from %s " //
                        + "where location = :location " //
                        + "and industry = :industry " //
                        + "and numEmpRange = :numEmpRange " //
                        + "and revRange = :revRange " //
                        + "and numLocRange = :numLocRange " //
                        + "and category = :category", //
                getEntityClass().getSimpleName());
        Query<AccountMasterFact> query = session.createQuery(queryStr, AccountMasterFact.class);
        query.setParameter("location", location);
        query.setParameter("industry", industry);
        query.setParameter("numEmpRange", numEmpRange);
        query.setParameter("revRange", revRange);
        query.setParameter("numLocRange", numLocRange);
        query.setParameter("category", category);
        List<?> results = query.list();
        if (results == null || results.isEmpty()) {
            return null;
        } else {
            return (AccountMasterFact) query.list().get(0);
        }
    }

}
