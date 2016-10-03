package com.latticeengines.datacloud.match.dao.impl;

import java.util.List;

import org.hibernate.Query;
import org.hibernate.Session;
import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.match.dao.AccountMasterColumnDao;
import com.latticeengines.db.exposed.dao.impl.BaseDaoWithAssignedSessionFactoryImpl;
import com.latticeengines.domain.exposed.datacloud.manage.AccountMasterColumn;

@Component("accountMasterColumnDao")
public class AccountMasterColumnDaoImpl extends BaseDaoWithAssignedSessionFactoryImpl<AccountMasterColumn>
        implements AccountMasterColumnDao {

    @Override
    protected Class<AccountMasterColumn> getEntityClass() {
        return AccountMasterColumn.class;
    }

    @SuppressWarnings("unchecked")
    @Override
    public List<AccountMasterColumn> findByTag(String tag) {
        Session session = getSessionFactory().getCurrentSession();
        String queryStr = String.format("from %s where groups like :tag", getEntityClass().getSimpleName());
        Query query = session.createQuery(queryStr);
        query.setParameter("tag", "%" + tag + "%");
        return query.list();

    }

    @SuppressWarnings("unchecked")
    @Override
    public List<AccountMasterColumn> findAllByVersion(String version) {
        Session session = getSessionFactory().getCurrentSession();
        String queryStr = String.format("from %s where dataCloudVersion = :version", getEntityClass().getSimpleName());
        Query query = session.createQuery(queryStr);
        query.setParameter("version", version);
        return query.list();
    }

}
