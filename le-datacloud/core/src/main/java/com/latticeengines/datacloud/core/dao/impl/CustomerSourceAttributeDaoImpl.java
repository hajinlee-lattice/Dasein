package com.latticeengines.datacloud.core.dao.impl;

import java.util.List;

import org.hibernate.Session;
import org.hibernate.query.Query;
import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.core.dao.CustomerSourceAttributeDao;
import com.latticeengines.db.exposed.dao.impl.BaseDaoWithAssignedSessionFactoryImpl;
import com.latticeengines.domain.exposed.datacloud.manage.CustomerSourceAttribute;

@Component("customerSourceAttributeDao")
public class CustomerSourceAttributeDaoImpl extends BaseDaoWithAssignedSessionFactoryImpl<CustomerSourceAttribute>
        implements CustomerSourceAttributeDao {
    @Override
    protected Class<CustomerSourceAttribute> getEntityClass() {
        return CustomerSourceAttribute.class;
    }

    @Override
    public List<CustomerSourceAttribute> getAttributes(String source, String stage, String transformer,
            String dataCloudVersion) {
        Session session = sessionFactory.getCurrentSession();
        String queryStr = String.format(
                "from %s where Stage = :stage And Transformer = :transformer And Source = :source And %s order by SourceAttributeID",
                getEntityClass().getSimpleName(),
                (dataCloudVersion == null ? "dataCloudVersion is null" : "dataCloudVersion = :version"));
        Query<CustomerSourceAttribute> query = session.createQuery(queryStr, CustomerSourceAttribute.class);
        query.setParameter("stage", stage);
        query.setParameter("transformer", transformer);
        query.setParameter("source", source);
        if (dataCloudVersion != null) {
            query.setParameter("version", dataCloudVersion);
        }
        return (List<CustomerSourceAttribute>) query.list();
    }
}
