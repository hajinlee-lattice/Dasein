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

    @Override
    public MetadataSegment findByQuerySourceAndName(String querySourceName, String name) {
        Session session = sessionFactory.getCurrentSession();
        Class<MetadataSegment> entityClz = getEntityClass();
        String queryStr = String
                .format("from %s as segment inner join fetch segment.querySource as querySource where querySource.name = '%s' and segment.name = '%s'",
                        entityClz.getSimpleName(), querySourceName, name);
        Query query = session.createQuery(queryStr);
        List list = query.list();
        if (list.size() == 0) {
            return null;
        }
        return (MetadataSegment) list.get(0);
    }

    @Override
    public MetadataSegment findByNameWithDefaultQuerySource(String name) {
        Session session = sessionFactory.getCurrentSession();
        Class<MetadataSegment> entityClz = getEntityClass();
        String queryStr = String
                .format("from %s as segment inner join fetch segment.querySource as querySource where querySource.isDefault = true and segment.name = '%s'",
                        entityClz.getSimpleName(), name);
        Query query = session.createQuery(queryStr);
        List list = query.list();
        if (list.size() == 0) {
            return null;
        }
        return (MetadataSegment) list.get(0);
    }
}
