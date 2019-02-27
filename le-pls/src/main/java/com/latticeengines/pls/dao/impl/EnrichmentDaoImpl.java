package com.latticeengines.pls.dao.impl;

import org.hibernate.Session;
import org.hibernate.query.Query;
import org.springframework.stereotype.Component;

import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.pls.Enrichment;
import com.latticeengines.pls.dao.EnrichmentDao;

@Component("enrichmentDao")
public class EnrichmentDaoImpl extends BaseDaoImpl<Enrichment> implements EnrichmentDao {

    @Override
    protected Class<Enrichment> getEntityClass() {
        return Enrichment.class;
    }

    @Override
    public void deleteEnrichmentById(String enrichmentId) {
        Session session = getSessionFactory().getCurrentSession();
        Class<Enrichment> entityClz = getEntityClass();
        String queryStr = String.format("delete from %s where pid = :enrichmentId",
                entityClz.getSimpleName());
        Query query = session.createQuery(queryStr);
        query.setString("enrichmentId", enrichmentId);
        query.executeUpdate();
    }

}
