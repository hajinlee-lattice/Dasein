package com.latticeengines.pls.dao;

import java.util.List;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.domain.exposed.pls.BucketMetadata;

public interface BucketMetadataDao extends BaseDao<BucketMetadata> {

    List<BucketMetadata> findBucketMetadatasForModelId(String modelId);

    List<BucketMetadata> findUpToDateBucketMetadatasForModelId(String modelId);

}
