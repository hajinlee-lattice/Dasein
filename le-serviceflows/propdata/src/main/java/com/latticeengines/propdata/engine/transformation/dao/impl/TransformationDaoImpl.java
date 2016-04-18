package com.latticeengines.propdata.engine.transformation.dao.impl;

import java.util.List;

import org.hibernate.Query;
import org.hibernate.Session;
import org.springframework.stereotype.Component;

import com.latticeengines.db.exposed.dao.impl.BaseDaoWithAssignedSessionFactoryImpl;
import com.latticeengines.domain.exposed.propdata.manage.Transformation;
import com.latticeengines.propdata.engine.transformation.dao.TransformationDao;

@Component("transformationDao")
public class TransformationDaoImpl extends BaseDaoWithAssignedSessionFactoryImpl<Transformation>
        implements TransformationDao {

    @Override
    protected Class<Transformation> getEntityClass() {
        return Transformation.class;
    }

    @SuppressWarnings("unchecked")
    @Override
    public List<Transformation> findAllForSource(String sourceName) {
        Session session = sessionFactory.getCurrentSession();
        String queryStr = String.format("from %s where SourceName = :sourceName", getEntityClass().getSimpleName());
        Query query = session.createQuery(queryStr);
        query.setString("sourceName", sourceName);
        return query.list();
    }

}
