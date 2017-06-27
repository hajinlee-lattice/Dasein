package com.latticeengines.metadata.dao.impl;

import java.util.List;

import org.hibernate.Query;
import org.hibernate.Session;
import org.springframework.stereotype.Component;

import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.metadata.dao.SegmentDao;

@Component("segmentDao")
public class SegmentDaoImpl extends BaseDaoImpl<MetadataSegment> implements SegmentDao {

    @Override
    protected Class<MetadataSegment> getEntityClass() {
        return MetadataSegment.class;
    }

    @SuppressWarnings("rawtypes")
    @Override
    public MetadataSegment findMasterSegment(String collectionName) {
        Session session = sessionFactory.getCurrentSession();
        Class<MetadataSegment> entityClz = getEntityClass();
        String queryPattern = "select seg from %s as seg";
        queryPattern += " join seg.dataCollection as dc";
        queryPattern += " where dc.name = :collectionName";
        queryPattern += " and seg.isMasterSegment is true";
        String queryStr = String.format(queryPattern, entityClz.getSimpleName());
        Query query = session.createQuery(queryStr);
        query.setParameter("collectionName", collectionName);
        List list = query.list();
        if (list.size() == 0) {
            return null;
        }
        return (MetadataSegment) list.get(0);
    }

}
