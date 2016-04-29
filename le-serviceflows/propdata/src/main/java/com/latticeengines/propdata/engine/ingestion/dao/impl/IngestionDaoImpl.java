package com.latticeengines.propdata.engine.ingestion.dao.impl;

import java.util.List;

import org.hibernate.Query;
import org.hibernate.Session;
import org.springframework.stereotype.Component;

import com.latticeengines.db.exposed.dao.impl.BaseDaoWithAssignedSessionFactoryImpl;
import com.latticeengines.domain.exposed.propdata.manage.Ingestion;
import com.latticeengines.propdata.engine.ingestion.dao.IngestionDao;

import reactor.util.CollectionUtils;

@Component("ingestionDao")
public class IngestionDaoImpl extends BaseDaoWithAssignedSessionFactoryImpl<Ingestion>
        implements IngestionDao {
    @Override
    protected Class<Ingestion> getEntityClass() {
        return Ingestion.class;
    }

    @SuppressWarnings("unchecked")
    @Override
    public Ingestion getIngestionByName(String name) {
        Session session = getSessionFactory().getCurrentSession();
        Class<Ingestion> entityClz = getEntityClass();
        String queryStr = String.format("from %s where IngestionName = :name",
                entityClz.getSimpleName());
        Query query = session.createQuery(queryStr);
        query.setParameter("name", name);
        List<Ingestion> resultList = query.list();
        if (CollectionUtils.isEmpty(resultList)) {
            return null;
        } else {
            return resultList.get(0);
        }
    }
}
