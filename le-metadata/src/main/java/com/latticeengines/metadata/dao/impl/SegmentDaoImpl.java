package com.latticeengines.metadata.dao.impl;

import java.util.List;

import org.hibernate.Query;
import org.hibernate.Session;
import org.springframework.stereotype.Component;

import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.metadata.DataCollectionType;
import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.metadata.dao.SegmentDao;

@Component("segmentDao")
public class SegmentDaoImpl extends BaseDaoImpl<MetadataSegment> implements SegmentDao {

    @Override
    protected Class<MetadataSegment> getEntityClass() {
        return MetadataSegment.class;
    }

    @Override
    public MetadataSegment findByDataCollectionAndName(String dataCollectionName, String name) {
        Session session = sessionFactory.getCurrentSession();
        Class<MetadataSegment> entityClz = getEntityClass();
        String queryStr = String.format(
                "from %s as segment inner join fetch segment.dataCollection as dataCollection where dataCollection.name = '%s' and segment.name = '%s'",
                entityClz.getSimpleName(), dataCollectionName, name);
        Query query = session.createQuery(queryStr);
        List list = query.list();
        if (list.size() == 0) {
            return null;
        }
        return (MetadataSegment) list.get(0);
    }

    @Override
    public MetadataSegment findByNameWithSegmentationDataCollection(String name) {
        Session session = sessionFactory.getCurrentSession();
        Class<MetadataSegment> entityClz = getEntityClass();
        String queryStr = String.format(
                "from %s as segment inner join fetch segment.dataCollection as dataCollection where dataCollection.type = '%s' and segment.name = '%s'",
                entityClz.getSimpleName(), DataCollectionType.Segmentation, name);
        Query query = session.createQuery(queryStr);
        List list = query.list();
        if (list.size() == 0) {
            return null;
        }
        return (MetadataSegment) list.get(0);
    }
}
