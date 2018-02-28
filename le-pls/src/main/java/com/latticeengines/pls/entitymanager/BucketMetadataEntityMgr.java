package com.latticeengines.pls.entitymanager;

import java.util.List;

import com.latticeengines.db.exposed.entitymgr.BaseEntityMgr;
import com.latticeengines.domain.exposed.pls.BucketMetadata;
import com.latticeengines.domain.exposed.pls.RatingEngine;

public interface BucketMetadataEntityMgr extends BaseEntityMgr<BucketMetadata> {

    List<BucketMetadata> getBucketMetadatasForModelId(String modelId);

    List<BucketMetadata> getBucketMetadatasBasedOnRatingEngine(RatingEngine ratingEngine);

    List<BucketMetadata> getUpToDateBucketMetadatasForModelId(String modelId);

    List<BucketMetadata> getUpToDateBucketMetadatasBasedOnRatingEngine(RatingEngine ratingEngine);

}
