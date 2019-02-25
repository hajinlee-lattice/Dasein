package com.latticeengines.datacloud.match.dao.impl;

import java.util.List;

import org.hibernate.Session;
import org.hibernate.query.Query;
import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.match.dao.ExternalColumnDao;
import com.latticeengines.db.exposed.dao.impl.BaseDaoWithAssignedSessionFactoryImpl;
import com.latticeengines.domain.exposed.datacloud.manage.ExternalColumn;

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
        String queryStr = String.format("from %s where Tags like :tag order by category asc, subcategory asc, PID asc",
                entityClz.getSimpleName());
        Query<ExternalColumn> query = session.createQuery(queryStr);
        query.setParameter("tag", "%" + tag + "%");
        return query.list();

    }

}
