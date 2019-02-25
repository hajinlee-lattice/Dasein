package com.latticeengines.pls.dao.impl;

import java.util.List;

import org.hibernate.Session;
import org.hibernate.query.Query;
import org.springframework.stereotype.Component;

import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.pls.Segment;
import com.latticeengines.pls.dao.PdSegmentDao;

@Component("pdSegmentDao")
public class PdSegmentDaoImpl extends BaseDaoImpl<Segment> implements PdSegmentDao {

    @Override
    protected Class<Segment> getEntityClass() {
        return Segment.class;
    }

    @SuppressWarnings("rawtypes")
    @Override
    public Segment findByName(String name) {
        Session session = getSessionFactory().getCurrentSession();
        Class<Segment> entityClz = getEntityClass();
        String queryStr = String.format("from %s where name = :segmentName", entityClz.getSimpleName());
        Query query = session.createQuery(queryStr);
        query.setString("segmentName", name);
        List list = query.list();
        if (list.size() == 0) {
            return null;
        }
        return (Segment) list.get(0);
    }

    @SuppressWarnings("rawtypes")
    @Override
    public Segment findByModelId(String modelId) {
        Session session = getSessionFactory().getCurrentSession();
        Class<Segment> entityClz = getEntityClass();
        String queryStr = String.format("from %s where model_id = :modelid", entityClz.getSimpleName());
        Query query = session.createQuery(queryStr);
        query.setString("modelid", modelId);
        List list = query.list();
        if (list.size() == 0) {
            return null;
        }
        return (Segment) list.get(0);
    }

}
