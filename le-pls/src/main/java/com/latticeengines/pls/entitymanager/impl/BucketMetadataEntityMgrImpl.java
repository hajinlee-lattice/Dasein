package com.latticeengines.pls.entitymanager.impl;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.db.exposed.entitymgr.impl.BaseEntityMgrImpl;
import com.latticeengines.domain.exposed.pls.BucketMetadata;
import com.latticeengines.pls.dao.BucketMetadataDao;
import com.latticeengines.pls.entitymanager.BucketMetadataEntityMgr;

@Component("bucketMetadataEntityMgr")
public class BucketMetadataEntityMgrImpl extends BaseEntityMgrImpl<BucketMetadata> implements BucketMetadataEntityMgr {

    @Autowired
    private BucketMetadataDao bucketMetadataDao;

    @Override
    public BaseDao<BucketMetadata> getDao() {
        return bucketMetadataDao;
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public List<BucketMetadata> getBucketMetadatasForModelId(String modelId) {
        return bucketMetadataDao.findBucketMetadatasForModelId(modelId);
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public List<BucketMetadata> getUpToDateBucketMetadatasForModelId(String modelId) {
        return bucketMetadataDao.findUpToDateBucketMetadatasForModelId(modelId);
    }

}
