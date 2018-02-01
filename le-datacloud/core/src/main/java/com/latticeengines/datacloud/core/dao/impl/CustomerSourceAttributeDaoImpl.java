package com.latticeengines.datacloud.core.dao.impl;

import java.util.List;

import org.hibernate.Query;
import org.hibernate.Session;
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

    @SuppressWarnings("unchecked")
    @Override
    public List<CustomerSourceAttribute> getAttributes(String source, String stage, String transformer,
            String dataCloudVersion) {
        Session session = sessionFactory.getCurrentSession();
        String queryStr = String.format(
                "from %s where Stage = :stage And Transformer = :transformer And Source = :source And %s order by SourceAttributeID",
                getEntityClass().getSimpleName(),
                (dataCloudVersion == null ? "dataCloudVersion is null" : "dataCloudVersion = :version"));
        Query query = session.createQuery(queryStr);
        query.setString("stage", stage);
        query.setString("transformer", transformer);
        query.setString("source", source);
        if (dataCloudVersion != null) {
            query.setString("version", dataCloudVersion);
        }
        return (List<CustomerSourceAttribute>) query.list();
    }
}
