package com.latticeengines.pls.dao;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.domain.exposed.pls.BucketMetadata;

import java.util.List;

public interface BucketMetadataDao extends BaseDao<BucketMetadata> {

    List<BucketMetadata> findBucketMetadatasForModelId(String modelId);

}
