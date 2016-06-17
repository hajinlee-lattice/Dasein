package com.latticeengines.propdata.match.dao.impl;

import java.util.List;

import org.hibernate.Query;
import org.hibernate.Session;
import org.springframework.stereotype.Component;

import com.latticeengines.db.exposed.dao.impl.BaseDaoWithAssignedSessionFactoryImpl;
import com.latticeengines.domain.exposed.propdata.manage.ExternalColumn;
import com.latticeengines.propdata.match.dao.ExternalColumnDao;

@Component("externalColumnDao")
public class ExternalColumnDaoImpl extends BaseDaoWithAssignedSessionFactoryImpl<ExternalColumn>
        implements ExternalColumnDao {

    @Override
    protected Class<ExternalColumn> getEntityClass() {
        return ExternalColumn.class;
    }

    @SuppressWarnings("unchecked")
    @Override
    public List<ExternalColumn> findByTag(String tag) {
        Session session = getSessionFactory().getCurrentSession();
        Class<ExternalColumn> entityClz = getEntityClass();
        String queryStr = String.format("from %s where Tags like :tag", entityClz.getSimpleName());
        Query query = session.createQuery(queryStr);
        query.setParameter("tag", "%" + tag + "%");
        return query.list();

    }

}
