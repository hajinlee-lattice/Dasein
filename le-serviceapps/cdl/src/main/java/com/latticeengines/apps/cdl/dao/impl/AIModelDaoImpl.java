package com.latticeengines.apps.cdl.dao.impl;

import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import org.apache.commons.collections4.CollectionUtils;
import org.hibernate.Session;
import org.hibernate.query.Query;
import org.springframework.stereotype.Component;

import com.latticeengines.apps.cdl.dao.AIModelDao;
import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.pls.AIModel;

import java.util.List;

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
        Query query = session.createQuery(queryStr);
        query.setParameter("id", id);
        List list =  query.list();
        if (CollectionUtils.size(list) != 1) {
            throw new RuntimeException(String.format("Found %d segments for rule based model %s, while it should be 1.", CollectionUtils.size(list), id));
        } else {
            return (MetadataSegment) list.get(0);
        }
    }

}
