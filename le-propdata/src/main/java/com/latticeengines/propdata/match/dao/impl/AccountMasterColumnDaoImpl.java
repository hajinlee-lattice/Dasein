package com.latticeengines.propdata.match.dao.impl;

import java.util.List;

import org.hibernate.Query;
import org.hibernate.Session;
import org.springframework.stereotype.Component;

import com.latticeengines.db.exposed.dao.impl.BaseDaoWithAssignedSessionFactoryImpl;
import com.latticeengines.domain.exposed.propdata.manage.AccountMasterColumn;
import com.latticeengines.propdata.match.dao.AccountMasterColumnDao;

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
        Class<AccountMasterColumn> entityClz = getEntityClass();
        String queryStr = String.format("from %s where Groups like :tag", entityClz.getSimpleName());
        Query query = session.createQuery(queryStr);
        query.setParameter("tag", "%" + tag + "%");
        return query.list();

    }

}
