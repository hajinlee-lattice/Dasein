package com.latticeengines.metadata.entitymgr.impl;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.db.exposed.entitymgr.impl.BaseEntityMgrImpl;
import com.latticeengines.domain.exposed.metadata.DataCollectionType;
import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.domain.exposed.metadata.MetadataSegmentProperty;
import com.latticeengines.metadata.dao.SegmentDao;
import com.latticeengines.metadata.dao.SegmentPropertyDao;
import com.latticeengines.metadata.entitymgr.DataCollectionEntityMgr;
import com.latticeengines.metadata.entitymgr.SegmentEntityMgr;
import com.latticeengines.security.exposed.util.MultiTenantContext;

@Component("segmentEntityMgr")
public class SegmentEntityMgrImpl extends BaseEntityMgrImpl<MetadataSegment> implements SegmentEntityMgr {

    @Autowired
    private SegmentDao segmentDao;

    @Autowired
    private DataCollectionEntityMgr dataCollectionEntityMgr;

    @Autowired
    private SegmentPropertyDao segmentPropertyDao;

    @Override
    public BaseDao<MetadataSegment> getDao() {
        return segmentDao;
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    @Override
    public MetadataSegment findByName(String name) {
        return segmentDao.findByNameWithSegmentationDataCollection(name);
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    @Override
    public List<MetadataSegment> findAll() {
        return super.findAll();
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    @Override
    public MetadataSegment findByName(String querySourceName, String name) {
        return segmentDao.findByDataCollectionAndName(querySourceName, name);
    }

    @Transactional(propagation = Propagation.REQUIRED)
    @Override
    public void createOrUpdate(MetadataSegment segment) {
        segment.setTenant(MultiTenantContext.getTenant());
        MetadataSegment existing = findByName(segment.getName());
        if (existing != null) {
            delete(existing);
        }
        if (segment.getDataCollection() == null) {
            segment.setDataCollection(dataCollectionEntityMgr.getDataCollection(DataCollectionType.Segmentation));
        }

        super.createOrUpdate(segment);
        for (MetadataSegmentProperty metadataSegmentProperty : segment.getMetadataSegmentProperties()) {
            metadataSegmentProperty.setMetadataSegment(segment);
            segmentPropertyDao.create(metadataSegmentProperty);
        }
    }

}
