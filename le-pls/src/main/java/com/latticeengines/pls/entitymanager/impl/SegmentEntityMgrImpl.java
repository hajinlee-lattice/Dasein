package com.latticeengines.pls.entitymanager.impl;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.db.exposed.entitymgr.impl.BaseEntityMgrImpl;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.pls.Segment;
import com.latticeengines.pls.dao.SegmentDao;
import com.latticeengines.pls.entitymanager.SegmentEntityMgr;

@Component("segmentEntityMgr")
public class SegmentEntityMgrImpl extends BaseEntityMgrImpl<Segment> implements SegmentEntityMgr {

    @Autowired
    private SegmentDao segmentDao;

    @Override
    public BaseDao<Segment> getDao() {
        return segmentDao;
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public List<Segment> getAll() {
        return super.findAll();
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public List<Segment> findAll() {
        return super.findAll();
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public Segment findByName(String segmentName) {
        return segmentDao.findByName(segmentName);
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public Segment findByModelId(String modelId) {
        return segmentDao.findByModelId(modelId);
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public void deleteByModelId(String modelId) {
        Segment segment = findByModelId(modelId);

        if (segment == null) {
            throw new LedpException(LedpCode.LEDP_18007, new String[] { modelId });
        }
        super.delete(segment);
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = false)
    public Segment retrieveByModelIdForInternalOperations(String modelId) {
        return segmentDao.findByModelId(modelId);
    }

}
