package com.latticeengines.pls.entitymanager;

import java.util.List;

import com.latticeengines.db.exposed.entitymgr.BaseEntityMgr;
import com.latticeengines.domain.exposed.pls.BucketMetadata;

public interface BucketMetadataEntityMgr extends BaseEntityMgr<BucketMetadata> {

    List<BucketMetadata> findBucketMetadatasForModelId(String modelId);

    List<BucketMetadata> findUpToDateBucketMetadatasForModelId(String modelId);

}
