package com.latticeengines.apps.cdl.dao.impl;

import java.util.List;

import org.apache.commons.collections4.CollectionUtils;
import org.hibernate.Session;
import org.hibernate.query.Query;
import org.springframework.stereotype.Component;

import com.latticeengines.apps.cdl.dao.AIModelDao;
import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.domain.exposed.pls.AIModel;

@Component("aiModelDao")
public class AIModelDaoImpl extends BaseDaoImpl<AIModel> implements AIModelDao {

    @Override
    protected Class<AIModel> getEntityClass() {
        return AIModel.class;
    }

    @Override
    public MetadataSegment findParentSegmentById(String id) {
        Session session = getSessionFactory().getCurrentSession();
        String queryPattern = "select model.ratingEngine.segment from %s as model";
        queryPattern += " where model.id = :id";
        String queryStr = String.format(queryPattern, getEntityClass().getSimpleName());
        @SuppressWarnings("unchecked")
        Query<MetadataSegment> query = session.createQuery(queryStr);
        query.setParameter("id", id);
        List<MetadataSegment> list = query.list();
        if (CollectionUtils.size(list) != 1) {
            throw new RuntimeException(
                    String.format("Found %d segments for AI model %s, while it should be 1.",
                            CollectionUtils.size(list), id));
        } else {
            return list.get(0);
        }
    }

    @Override
    public void create(AIModel aiModel) {
        int maxIteration = findMaxIterationByRatingEngineId(aiModel.getRatingEngine().getId());
        int latestIteration = aiModel.getRatingEngine().getLatestIteration() != null
                ? aiModel.getRatingEngine().getLatestIteration().getIteration()
                : 0;
        if (maxIteration == latestIteration) {
            aiModel.setIteration(maxIteration + 1);
            super.create(aiModel);
        } else {
            throw new RuntimeException(
                    String.format("Latest iteration for AI model is %d, while it should be %d.",
                            latestIteration, maxIteration));
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public int findMaxIterationByRatingEngineId(String ratineEngindId) {
        Session session = getSessionFactory().getCurrentSession();
        String queryPattern = "select max(model.iteration) from %s as model where model.ratingEngine.id = :id";
        String queryStr = String.format(queryPattern, getEntityClass().getSimpleName());
        Query<Integer> query = session.createQuery(queryStr);
        query.setParameter("id", ratineEngindId);
        return query.uniqueResult() != null ? query.uniqueResult() : 0;
    }

}
