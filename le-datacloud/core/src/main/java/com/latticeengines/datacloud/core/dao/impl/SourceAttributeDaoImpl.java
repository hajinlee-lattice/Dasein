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

    @Override
    public String getLatestDataCloudVersion(String sourceName, String stage, String transformer) {
        Session session = sessionFactory.getCurrentSession();
        // 1. dataCloudVersion = "<majorVersion>.<minorVersion>"
        // 2. SUBSTRING(dataCloudVersion, ...) to get minorVersion. Note that index start with 1 in HQL and we need to
        //    increase one more to skip '.', so there is + 2 : we hard code it to 5 for now 
        //    [E.g. : for 2.0.18 : (2.0) = 3 + 1 (.) + 1 (since index starts from 1)]
        // 3. cast as int so that it is sorted correctly. E.g., 2.0.10 > 2.0.5
        String queryStr = String.format(
                "select distinct dataCloudVersion from %s where Stage = :stage and Transformer = :transformer and Source = :source "
                            + "order by CAST(SUBSTRING(dataCloudVersion, 5) as int) desc",
                getEntityClass().getSimpleName());
        Query<String> query = session.createQuery(queryStr, String.class);
        query.setParameter("stage", stage);
        query.setParameter("transformer", transformer);
        query.setParameter("source", sourceName);
        List<?> results = query.list();
        if (results == null || results.isEmpty()) {
            return null;
        } else {
            return (String) query.list().get(0);
        }
    }
}
