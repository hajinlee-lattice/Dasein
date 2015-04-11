package com.latticeengines.pls.entitymanager.impl;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.domain.exposed.pls.Segment;
import com.latticeengines.pls.dao.SegmentDao;
import com.latticeengines.pls.entitymanager.SegmentEntityMgr;

@Component("segmentEntityMgr")
public class SegmentEntityMgrImpl extends BasePLSEntityMgrImpl<Segment> implements SegmentEntityMgr {

    @Autowired
    private SegmentDao segmentDao;
    
    @Override
    public BaseDao<Segment> getDao() {
        return segmentDao;
    }

    @Override
    @Transactional(value = "pls", propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public List<Segment> getAll() {
        return super.findAll();
    }
    
    @Override
    @Transactional(value = "pls", propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public List<Segment> findAll() {
        return super.findAll();
    }


}
