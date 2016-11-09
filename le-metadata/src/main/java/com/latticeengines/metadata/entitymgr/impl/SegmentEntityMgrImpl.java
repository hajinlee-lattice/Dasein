package com.latticeengines.metadata.entitymgr.impl;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.db.exposed.entitymgr.impl.BaseEntityMgrImpl;
import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.metadata.dao.SegmentDao;
import com.latticeengines.metadata.entitymgr.SegmentEntityMgr;

@Component("segmentEntityMgr")
public class SegmentEntityMgrImpl extends BaseEntityMgrImpl<MetadataSegment> implements SegmentEntityMgr {
    
    @Autowired
    private SegmentDao segmentDao;

    @Override
    public BaseDao<MetadataSegment> getDao() {
        return segmentDao;
    }

    @Override
    public MetadataSegment findByName(String name) {
        return segmentDao.findByField("NAME", name);
    }

}
