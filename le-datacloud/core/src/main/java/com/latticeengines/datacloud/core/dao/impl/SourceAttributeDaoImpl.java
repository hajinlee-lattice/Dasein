package com.latticeengines.datacloud.core.dao.impl;

import java.util.List;

import org.hibernate.Query;
import org.hibernate.Session;
import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.core.dao.SourceAttributeDao;
import com.latticeengines.db.exposed.dao.impl.BaseDaoWithAssignedSessionFactoryImpl;
import com.latticeengines.domain.exposed.datacloud.manage.SourceAttribute;

@Component("sourceAttributeDao")
public class SourceAttributeDaoImpl extends BaseDaoWithAssignedSessionFactoryImpl<SourceAttribute>
        implements SourceAttributeDao {

    @Override
    protected Class<SourceAttribute> getEntityClass() {
        return SourceAttribute.class;
    }

    @SuppressWarnings("unchecked")
    @Override
    public List<SourceAttribute> getAttributes(String source, String stage, String transformer) {
        Session session = sessionFactory.getCurrentSession();
        String queryStr = String.format(
                "from %s where Stage = :stage And Transformer = :transformer And Source = :source order by SourceAttributeID",
                getEntityClass().getSimpleName());
        Query query = session.createQuery(queryStr);
        query.setString("stage", stage);
        query.setString("transformer", transformer);
        query.setString("source", source);
        return (List<SourceAttribute>) query.list();
    }
}
