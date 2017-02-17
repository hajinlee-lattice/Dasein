package com.latticeengines.metadata.entitymgr.impl;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.db.exposed.entitymgr.impl.BaseEntityMgrImpl;
import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.metadata.dao.SegmentDao;
import com.latticeengines.metadata.entitymgr.QuerySourceEntityMgr;
import com.latticeengines.metadata.entitymgr.SegmentEntityMgr;

@Component("segmentEntityMgr")
public class SegmentEntityMgrImpl extends BaseEntityMgrImpl<MetadataSegment> implements SegmentEntityMgr {

    @Autowired
    private SegmentDao segmentDao;

    @Autowired
    private QuerySourceEntityMgr querySourceEntityMgr;

    @Override
    public BaseDao<MetadataSegment> getDao() {
        return segmentDao;
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    @Override
    public MetadataSegment findByName(String name) {
        return segmentDao.findByNameWithDefaultQuerySource(name);
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    @Override
    public MetadataSegment findByName(String querySourceName, String name) {
        return segmentDao.findByQuerySourceAndName(querySourceName, name);
    }

    @Transactional(propagation = Propagation.REQUIRED)
    @Override
    public void createOrUpdate(MetadataSegment segment) {
        MetadataSegment existing = findByName(segment.getName());
        if (existing != null) {
            delete(existing);
        }
        if (segment.getQuerySource() == null) {
            segment.setQuerySource(querySourceEntityMgr.getDefaultQuerySource());
        }

        super.createOrUpdate(segment);
    }

}
