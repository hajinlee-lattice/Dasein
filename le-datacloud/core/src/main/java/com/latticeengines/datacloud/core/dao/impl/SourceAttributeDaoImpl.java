package com.latticeengines.datacloud.core.dao.impl;

import java.util.List;

import org.hibernate.Session;
import org.hibernate.query.Query;
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

    @Override
    public List<SourceAttribute> getAttributes(String source, String stage, String transformer) {
        return getAttributes(source, stage, transformer, null);
    }

    @Override
    public List<SourceAttribute> getAttributes(String source, String stage, String transformer,
            String dataCloudVersion) {
        Session session = sessionFactory.getCurrentSession();
        String queryStr = String.format(
                "from %s where Stage = :stage And Transformer = :transformer And Source = :source And %s order by SourceAttributeID",
                getEntityClass().getSimpleName(),
                (dataCloudVersion == null ? "dataCloudVersion is null" : "dataCloudVersion = :version"));
        Query<SourceAttribute> query = session.createQuery(queryStr, SourceAttribute.class);
        query.setParameter("stage", stage);
        query.setParameter("transformer", transformer);
        query.setParameter("source", source);
        if (dataCloudVersion != null) {
            query.setParameter("version", dataCloudVersion);
        }
        return (List<SourceAttribute>) query.list();
    }
}
