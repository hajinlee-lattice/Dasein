package com.latticeengines.apps.cdl.dao.impl;

import java.util.List;

import org.apache.commons.collections4.CollectionUtils;
import org.hibernate.Session;
import org.hibernate.query.Query;
import org.springframework.stereotype.Component;

import com.latticeengines.apps.cdl.dao.RuleBasedModelDao;
import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.domain.exposed.pls.RuleBasedModel;

@Component("ruleBasedModelDao")
public class RuleBasedModelDaoImpl extends BaseDaoImpl<RuleBasedModel> implements RuleBasedModelDao {

    @Override
    protected Class<RuleBasedModel> getEntityClass() {
        return RuleBasedModel.class;
    }

    @Override
    public RuleBasedModel findById(String id) {
        return super.findByField("ID", id);
    }

    @SuppressWarnings("unchecked")
    @Override
    public MetadataSegment findParentSegmentById(String id) {
        Session session = getSessionFactory().getCurrentSession();
        String queryPattern = "select model.ratingEngine.segment from %s as model";
        queryPattern += " where model.id = :id";
        String queryStr = String.format(queryPattern, getEntityClass().getSimpleName());
        Query<MetadataSegment> query = session.createQuery(queryStr);
        query.setParameter("id", id);
        List<MetadataSegment> list = query.list();
        if (CollectionUtils.size(list) != 1) {
            throw new RuntimeException(String.format("Found %d segments for rule based model %s, while it should be 1.", CollectionUtils.size(list), id));
        } else {
            return list.get(0);
        }
    }
}
