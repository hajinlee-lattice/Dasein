package com.latticeengines.datacloud.etl.dao.impl;

import java.util.List;

import org.hibernate.query.Query;
import org.hibernate.Session;
import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.etl.dao.SourceColumnDao;
import com.latticeengines.db.exposed.dao.impl.BaseDaoWithAssignedSessionFactoryImpl;
import com.latticeengines.domain.exposed.datacloud.manage.SourceColumn;

@Component
public class SourceColumnDaoImpl extends BaseDaoWithAssignedSessionFactoryImpl<SourceColumn>
        implements SourceColumnDao {

    @Override
    protected Class<SourceColumn> getEntityClass() {
        return SourceColumn.class;
    }

    @SuppressWarnings("unchecked")
    @Override
    public List<SourceColumn> getColumnsOfSource(String sourceName) {
        Session session = sessionFactory.getCurrentSession();
        String queryStr = String.format("from %s where SourceName = :sourceName order by SourceColumnID",
                getEntityClass().getSimpleName());
        Query query = session.createQuery(queryStr);
        query.setString("sourceName", sourceName);
        return (List<SourceColumn>) query.list();
    }

}
