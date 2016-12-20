package com.latticeengines.datacloud.core.dao.impl;

import java.util.List;

import org.hibernate.Query;
import org.hibernate.Session;

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
        Query query = session.createQuery(queryStr);
        query.setLong("location", location);
        query.setLong("industry", industry);
        query.setLong("numEmpRange", numEmpRange);
        query.setLong("revRange", revRange);
        query.setLong("numLocRange", numLocRange);
        query.setLong("category", category);
        List<?> results = query.list();
        if (results == null || results.isEmpty()) {
            return null;
        } else {
            return (AccountMasterFact) query.list().get(0);
        }
    }

}
