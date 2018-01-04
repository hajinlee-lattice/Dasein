package com.latticeengines.modelquality.dao.impl;

import java.util.List;

import org.hibernate.Query;
import org.hibernate.Session;
import org.springframework.stereotype.Component;

import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.modelquality.AnalyticPipeline;
import com.latticeengines.modelquality.dao.AnalyticPipelineDao;

@Component("qualityAnalyticPipelineDao")
public class AnalyticPipelineDaoImpl extends ModelQualityBaseDaoImpl<AnalyticPipeline> implements AnalyticPipelineDao {

    @Override
    protected Class<AnalyticPipeline> getEntityClass() {
        return AnalyticPipeline.class;
    }

    @Override
    public AnalyticPipeline findByMaxVersion() {
        Session session = getSessionFactory().getCurrentSession();
        String queryStr = String.format("from %s where version = (select MAX(version) from %s)",
                getEntityClass().getSimpleName(), getEntityClass().getSimpleName());
        Query query = session.createQuery(queryStr);
        @SuppressWarnings("unchecked")
        List<AnalyticPipeline> results = query.list();
        if (results.size() == 0) {
            return null;
        }
        if (results.size() > 1) {
            throw new RuntimeException("Multiple rows found with the same value for VERSION");
        }
        return results.get(0);
    }
}
