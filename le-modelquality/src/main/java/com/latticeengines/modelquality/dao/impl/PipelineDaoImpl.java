package com.latticeengines.modelquality.dao.impl;

import java.util.List;

import org.hibernate.Session;
import org.hibernate.query.Query;
import org.springframework.stereotype.Component;

import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.modelquality.Pipeline;
import com.latticeengines.modelquality.dao.PipelineDao;

@Component("pipelineDao")
public class PipelineDaoImpl extends BaseDaoImpl<Pipeline> implements PipelineDao {

    @Override
    protected Class<Pipeline> getEntityClass() {
        return Pipeline.class;
    }

    @Override
    public Pipeline findByMaxVersion() {
        Session session = getSessionFactory().getCurrentSession();
        String queryStr = String.format("from %s where version = (select MAX(version) from %s)",
                getEntityClass().getSimpleName(), getEntityClass().getSimpleName());
        Query<Pipeline> query = session.createQuery(queryStr, Pipeline.class);
        List<Pipeline> results = query.list();
        if (results.size() == 0) {
            return null;
        }
        if (results.size() > 1) {
            throw new RuntimeException("Multiple rows found with the same value for VERSION");
        }
        return results.get(0);
    }
}
